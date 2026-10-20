//! Continuous OTLP/HTTP JSON profiles export (development schema version 1).
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::StackTrace;

/// Configuration for continuous profiling. The endpoint is the complete profiles URL.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Config {
    pub endpoint: String,
    pub service_name: String,
    pub interval: Duration,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.endpoint).context("Invalid OTLP endpoint")?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "OTLP endpoint must be an HTTP(S) URL"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
            "OTLP endpoint must not contain credentials or a fragment"
        );
        ensure!(
            !self.interval.is_zero() && self.interval <= Duration::from_secs(86400),
            "OTLP export interval must be between 0 and 86400 seconds (exclusive of zero)"
        );
        ensure!(
            !self.service_name.is_empty(),
            "OTLP service name must not be empty"
        );
        Ok(())
    }
}

// Bound both each batch and the queue, even if a collector is unavailable.
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const MAX_SAMPLES: usize = 10_000;

pub(crate) struct Exporter {
    sender: Option<SyncSender<Vec<StackTrace>>>,
    worker: Option<JoinHandle<Result<()>>>,
    traces: Vec<StackTrace>,
    bytes: usize,
    dropped: usize,
}

impl Exporter {
    pub(crate) fn new(config: Config, rate: u32, on_cpu: bool) -> Result<Self> {
        config.validate()?;
        ensure!(
            (1..=1_000_000_000).contains(&rate),
            "Sample rate must be between 1 and 1000000000"
        );
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let (sender, receiver) = sync_channel::<Vec<StackTrace>>(2);
        let worker = std::thread::spawn(move || {
            let mut failures = 0;
            for batch in receiver {
                if let Err(error) = send(
                    &client,
                    &config.endpoint,
                    &encode(&batch, &config.service_name, rate, on_cpu),
                ) {
                    eprintln!(
                        "OTLP export failed; dropping {} samples: {error:#}",
                        batch.len()
                    );
                    failures += 1;
                }
            }
            ensure!(
                failures == 0,
                "{failures} OTLP profile batches could not be exported"
            );
            Ok(())
        });
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            traces: Vec::new(),
            bytes: 0,
            dropped: 0,
        })
    }

    pub(crate) fn record(&mut self, trace: &StackTrace) {
        let bytes = std::mem::size_of::<StackTrace>()
            + trace
                .trace
                .iter()
                .map(|f| {
                    std::mem::size_of::<crate::StackFrame>()
                        + f.name.len()
                        + f.relative_path.len()
                        + f.absolute_path.as_ref().map_or(0, String::len)
                })
                .sum::<usize>();
        if bytes > MAX_BATCH_BYTES {
            self.dropped += 1;
            eprintln!("OTLP sample exceeds batch memory limit; dropping sample");
            return;
        }
        if self.bytes + bytes > MAX_BATCH_BYTES || self.traces.len() >= MAX_SAMPLES {
            self.flush();
        }
        self.bytes += bytes;
        self.traces.push(trace.clone());
    }

    pub(crate) fn flush(&mut self) {
        if self.traces.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.traces);
        self.bytes = 0;
        let count = batch.len();
        if self.sender.as_ref().unwrap().try_send(batch).is_err() {
            self.dropped += count;
            eprintln!("OTLP export queue unavailable; dropping {count} samples");
        }
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        // The final partial batch waits for queue space; requests have bounded timeouts.
        if !self.traces.is_empty() {
            self.sender
                .as_ref()
                .unwrap()
                .send(std::mem::take(&mut self.traces))
                .context("OTLP worker stopped")?;
        }
        self.sender.take();
        let result = self
            .worker
            .take()
            .unwrap()
            .join()
            .map_err(|_| anyhow::anyhow!("OTLP worker panicked"))?;
        ensure!(
            self.dropped == 0,
            "{} OTLP samples were dropped",
            self.dropped
        );
        result
    }
}

impl Drop for Exporter {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn send(client: &reqwest::blocking::Client, endpoint: &str, body: &Value) -> Result<()> {
    // A bounded retry on transient OTLP HTTP statuses. Never retry partial success.
    for attempt in 0..3 {
        let response = client
            .post(endpoint)
            .header("OTLP-Profiles-Development-Version", "1")
            .json(body)
            .send();
        let mut response = match response {
            Ok(response) => response,
            Err(error) if attempt == 2 => return Err(error.into()),
            Err(_) => {
                std::thread::sleep(Duration::from_millis(200 << attempt));
                continue;
            }
        };
        let status = response.status();
        if matches!(status.as_u16(), 429 | 502 | 503 | 504) && attempt < 2 {
            let delay = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            // Don't retry sooner than requested when Retry-After exceeds our shutdown budget.
            if delay.is_some_and(|seconds| seconds > 5) {
                bail!("Collector requested a retry delay exceeding 5 seconds");
            }
            std::thread::sleep(
                delay
                    .map(Duration::from_secs)
                    .unwrap_or(Duration::from_millis(200 << attempt)),
            );
            continue;
        }
        ensure!(status.as_u16() == 200, "Collector returned HTTP {status}");
        let mut bytes = Vec::new();
        (&mut response)
            .take(64 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 64 * 1024, "OTLP response exceeds 64 KiB");
        let result: Value = serde_json::from_slice(&bytes).context("Invalid OTLP JSON response")?;
        ensure!(result.is_object(), "Expected an OTLP response object");
        if let Some(partial) = result.get("partialSuccess") {
            let rejected = partial
                .get("rejectedProfiles")
                .map(|n| {
                    n.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| n.to_string())
                })
                .unwrap_or_else(|| "0".into());
            let message = partial
                .get("errorMessage")
                .and_then(Value::as_str)
                .unwrap_or("");
            ensure!(
                rejected == "0",
                "Collector rejected {rejected} profiles: {message}"
            );
            if !message.is_empty() {
                eprintln!("OTLP collector warning: {message}");
            }
        }
        return Ok(());
    }
    unreachable!()
}

#[derive(Default)]
struct Table {
    values: Vec<Value>,
    indices: HashMap<String, i32>,
}
impl Table {
    fn new(zero: Value) -> Self {
        let mut t = Self::default();
        t.intern(zero);
        t
    }
    fn intern(&mut self, value: Value) -> i32 {
        let key = value.to_string();
        if let Some(index) = self.indices.get(&key) {
            return *index;
        }
        let index = self.values.len() as i32;
        self.indices.insert(key, index);
        self.values.push(value);
        index
    }
}

fn nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

// Field names and integer encodings follow OTLP JSON mapping. See docs/continuous-profiling.md
// for the pinned schema provenance. rbspy stacks already have the leaf frame first.
fn encode(traces: &[StackTrace], service: &str, rate: u32, on_cpu: bool) -> Value {
    let mut strings = Table::new(json!(""));
    let mut functions = Table::new(json!({}));
    let mut locations = Table::new(json!({}));
    let mut stacks = Table::new(json!({}));
    let mut attributes = Table::new(json!({}));
    let kind = strings.intern(json!(if on_cpu { "cpu" } else { "wall" }));
    let unit = strings.intern(json!("nanoseconds"));
    let thread_key = strings.intern(json!("thread.id"));
    let frame_key = strings.intern(json!("profile.frame.type"));
    let frame_attr =
        attributes.intern(json!({"keyStrindex": frame_key, "value": {"stringValue": "ruby"}}));
    let period = 1_000_000_000u64 / u64::from(rate);
    let mut groups: BTreeMap<Option<crate::Pid>, Vec<&StackTrace>> = BTreeMap::new();
    for trace in traces {
        groups.entry(trace.pid).or_default().push(trace);
    }
    let resources: Vec<Value> = groups.into_iter().map(|(pid, traces)| {
        let mut start = u64::MAX;
        let mut end = 0;
        let mut samples: BTreeMap<(i32, Vec<i32>), (Vec<String>, Vec<String>)> = BTreeMap::new();
        for trace in traces {
            let indices: Vec<i32> = trace.trace.iter().map(|f| {
                let name = strings.intern(json!(f.name));
                let file = strings.intern(json!(f.path()));
                let function = functions.intern(json!({"nameStrindex": name, "filenameStrindex": file}));
                locations.intern(json!({"lines": [{"functionIndex": function, "line": f.lineno.unwrap_or(0).to_string()}], "attributeIndices": [frame_attr]}))
            }).collect();
            let stack = if indices.is_empty() { 0 } else { stacks.intern(json!({"locationIndices": indices})) };
            let attrs = trace.thread_id.map(|id| vec![attributes.intern(json!({"keyStrindex": thread_key, "value": {"intValue": id.to_string()}}))]).unwrap_or_default();
            let timestamp = nanos(trace.time.unwrap_or_else(SystemTime::now));
            start = start.min(timestamp);
            end = end.max(timestamp.saturating_add(period));
            let (values, times) = samples.entry((stack, attrs)).or_default();
            values.push(period.to_string());
            times.push(timestamp.to_string());
        }
        let samples: Vec<Value> = samples.into_iter().map(|((stack, attrs), (values, times))| json!({"stackIndex": stack, "attributeIndices": attrs, "values": values, "timestampsUnixNano": times})).collect();
        let mut resource_attrs = vec![json!({"key": "service.name", "value": {"stringValue": service}}), json!({"key": "process.runtime.name", "value": {"stringValue": "ruby"}})];
        if let Some(pid) = pid { resource_attrs.push(json!({"key": "process.pid", "value": {"intValue": pid.to_string()}})); }
        json!({"resource": {"attributes": resource_attrs}, "scopeProfiles": [{"scope": {"name": "rbspy", "version": env!("CARGO_PKG_VERSION")}, "profiles": [{"sampleType": {"typeStrindex": kind, "unitStrindex": unit}, "periodType": {"typeStrindex": kind, "unitStrindex": unit}, "period": period.to_string(), "timeUnixNano": start.to_string(), "durationNano": end.saturating_sub(start).to_string(), "samples": samples}]}]})
    }).collect();
    json!({"resourceProfiles": resources, "dictionary": {"stringTable": strings.values, "functionTable": functions.values, "locationTable": locations.values, "stackTable": stacks.values, "attributeTable": attributes.values, "mappingTable": [{}], "linkTable": [{"traceId": "00000000000000000000000000000000", "spanId": "0000000000000000"}]}})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    fn trace(pid: crate::Pid, thread: usize) -> StackTrace {
        StackTrace {
            pid: Some(pid),
            thread_id: Some(thread),
            time: Some(UNIX_EPOCH + Duration::from_secs(10)),
            on_cpu: Some(true),
            trace: vec![crate::StackFrame {
                name: "work".into(),
                relative_path: "app.rb".into(),
                absolute_path: None,
                lineno: Some(42),
            }],
        }
    }

    #[test]
    fn dictionary_references_and_process_identity() {
        let request = encode(
            &[trace(7, 1), trace(7, 1), trace(8, 2)],
            "checkout",
            100,
            false,
        );
        let dict = &request["dictionary"];
        assert_eq!(dict["stringTable"][0], "");
        for name in [
            "functionTable",
            "locationTable",
            "stackTable",
            "attributeTable",
            "mappingTable",
        ] {
            assert_eq!(dict[name][0], json!({}));
        }
        assert_eq!(request["resourceProfiles"].as_array().unwrap().len(), 2);
        let profile = &request["resourceProfiles"][0]["scopeProfiles"][0]["profiles"][0];
        let kind = profile["sampleType"]["typeStrindex"].as_u64().unwrap() as usize;
        assert_eq!(dict["stringTable"][kind], "wall");
        assert_eq!(profile["period"], "10000000");
        assert_eq!(profile["samples"].as_array().unwrap().len(), 1);
        let sample = &profile["samples"][0];
        assert_eq!(sample["values"], json!(["10000000", "10000000"]));
        assert_eq!(
            sample["timestampsUnixNano"],
            json!(["10000000000", "10000000000"])
        );
        let stack = sample["stackIndex"].as_u64().unwrap() as usize;
        let location = dict["stackTable"][stack]["locationIndices"][0]
            .as_u64()
            .unwrap() as usize;
        assert_eq!(dict["locationTable"][location]["lines"][0]["line"], "42");
        let function = dict["locationTable"][location]["lines"][0]["functionIndex"]
            .as_u64()
            .unwrap() as usize;
        let name = dict["functionTable"][function]["nameStrindex"]
            .as_u64()
            .unwrap() as usize;
        assert_eq!(dict["stringTable"][name], "work");
        assert_eq!(dict["locationTable"].as_array().unwrap().len(), 2);
    }

    // A real HTTP socket verifies headers, JSON encoding, retries and final delivery.
    fn server(responses: Vec<(&'static str, &'static str)>) -> (String, JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!(
            "http://{}/v1development/profiles",
            listener.local_addr().unwrap()
        );
        let worker = std::thread::spawn(move || {
            let mut bodies = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(&socket);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(line.starts_with("POST /v1development/profiles HTTP/1.1"));
                let mut headers = String::new();
                let mut length = 0;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                    headers.push_str(&line.to_lowercase());
                }
                assert!(headers.contains("content-type: application/json"));
                assert!(headers.contains("otlp-profiles-development-version: 1"));
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).unwrap();
                bodies.push(serde_json::from_slice(&bytes).unwrap());
                write!(socket, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
            bodies
        });
        (endpoint, worker)
    }

    #[test]
    fn retries_and_flushes_final_batch() {
        let (endpoint, server) = server(vec![
            ("503 Unavailable", "{}"),
            ("200 OK", "{}"),
            ("200 OK", "{}"),
        ]);
        let mut exporter = Exporter::new(
            Config {
                endpoint,
                service_name: "test".into(),
                interval: Duration::from_secs(60),
            },
            100,
            true,
        )
        .unwrap();
        exporter.record(&trace(7, 1));
        exporter.flush();
        exporter.record(&trace(8, 1));
        exporter.finish().unwrap();
        let bodies = server.join().unwrap();
        assert_eq!(bodies[0], bodies[1]);
        assert_ne!(bodies[1], bodies[2]);
    }

    #[test]
    fn partial_success_is_reported_without_retry() {
        let (endpoint, server) = server(vec![(
            "200 OK",
            r#"{"partialSuccess":{"rejectedProfiles":"1","errorMessage":"unsupported"}}"#,
        )]);
        let error = send(&reqwest::blocking::Client::new(), &endpoint, &json!({})).unwrap_err();
        assert!(error.to_string().contains("rejected 1"));
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[test]
    fn permanent_errors_are_not_retried() {
        let (endpoint, server) = server(vec![("400 Bad Request", "{}")]);
        assert!(send(&reqwest::blocking::Client::new(), &endpoint, &json!({})).is_err());
        assert_eq!(server.join().unwrap().len(), 1);
    }

    #[test]
    #[ignore = "requires a running profiles-capable Collector and RBSPY_TEST_OTLP_ENDPOINT"]
    fn collector_integration() {
        let endpoint = std::env::var("RBSPY_TEST_OTLP_ENDPOINT").unwrap();
        let mut exporter = Exporter::new(
            Config {
                endpoint,
                service_name: "rbspy-integration".into(),
                interval: Duration::from_secs(1),
            },
            100,
            true,
        )
        .unwrap();
        let mut first = trace(7, 1);
        first.time = Some(SystemTime::now());
        exporter.record(&first);
        exporter.flush();
        let mut last = trace(8, 2);
        last.time = Some(SystemTime::now());
        exporter.record(&last);
        exporter.finish().unwrap();
    }

    #[test]
    #[ignore = "requires Collector -> Pyroscope, RBSPY_TEST_OTLP_ENDPOINT and RBSPY_TEST_PYROSCOPE_URL"]
    fn pyroscope_integration() {
        let endpoint = std::env::var("RBSPY_TEST_OTLP_ENDPOINT").unwrap();
        let backend = std::env::var("RBSPY_TEST_PYROSCOPE_URL").unwrap();
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        for on_cpu in [true, false] {
            let kind = if on_cpu { "cpu" } else { "wall" };
            let start = SystemTime::now();
            let service = format!("rbspy-integration-{kind}-{}", nanos(start));
            let mut exporter = Exporter::new(
                Config {
                    endpoint: endpoint.clone(),
                    service_name: service.clone(),
                    interval: Duration::from_secs(1),
                },
                100,
                on_cpu,
            )
            .unwrap();
            let mut first = trace(7, 1);
            first.time = Some(start);
            exporter.record(&first);
            exporter.flush();
            let mut last = trace(8, 2);
            last.time = Some(SystemTime::now());
            exporter.record(&last);
            exporter.finish().unwrap();
            let mut result = json!({});
            for _ in 0..30 {
                result = client
                    .post(format!(
                        "{}/querier.v1.QuerierService/SelectMergeStacktraces",
                        backend.trim_end_matches('/')
                    ))
                    .json(&json!({
                        "start": nanos(start) / 1_000_000 - 1000,
                        "end": nanos(SystemTime::now()) / 1_000_000 + 1000,
                        "labelSelector": format!("{{service_name=\"{service}\"}}"),
                        "profileTypeID": format!("{kind}:{kind}:nanoseconds:{kind}:nanoseconds"),
                    }))
                    .send()
                    .unwrap()
                    .error_for_status()
                    .unwrap()
                    .json()
                    .unwrap();
                if result["flamegraph"]["total"] == "20000000" {
                    break;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            assert_eq!(
                result["flamegraph"]["total"], "20000000",
                "{kind}: {result}"
            );
            assert!(
                result["flamegraph"]["names"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("work")),
                "{kind}: {result}"
            );
        }
    }

    #[test]
    fn queue_and_batch_limits_bound_memory() {
        let (sender, receiver) = sync_channel(1);
        let mut exporter = Exporter {
            sender: Some(sender),
            worker: None,
            traces: Vec::new(),
            bytes: 0,
            dropped: 0,
        };
        exporter.flush();
        assert!(receiver.try_recv().is_err());
        for _ in 0..=MAX_SAMPLES {
            exporter.record(&trace(7, 1));
        }
        assert_eq!(exporter.traces.len(), 1);
        // The first full batch occupies the queue. The second cannot block sampling.
        exporter.flush();
        assert_eq!(exporter.dropped, 1);
        assert!(exporter.traces.is_empty());
        assert_eq!(receiver.recv().unwrap().len(), MAX_SAMPLES);
        let mut oversized = trace(7, 1);
        oversized.trace[0].name = "x".repeat(MAX_BATCH_BYTES);
        exporter.record(&oversized);
        assert!(exporter.traces.is_empty());
        assert_eq!(exporter.dropped, 2);
    }

    #[test]
    fn validates_configuration() {
        let mut config = Config {
            endpoint: "http://localhost:4318/v1development/profiles".into(),
            service_name: "ruby".into(),
            interval: Duration::from_secs(60),
        };
        config.validate().unwrap();
        config.interval = Duration::ZERO;
        assert!(config.validate().is_err());
        config.interval = Duration::from_secs(1);
        config.endpoint = "file:///tmp/profile".into();
        assert!(config.validate().is_err());
    }
}
