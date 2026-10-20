# Continuous profiling with OpenTelemetry

Send a running Ruby process's profiles to an OpenTelemetry Collector:

```sh
rbspy record --pid 1234 \
  --otlp-endpoint http://localhost:4318/v1development/profiles \
  --service-name checkout --otlp-interval 60
```

You can also launch a command:

```sh
rbspy record --otlp-endpoint http://localhost:4318/v1development/profiles \
  --service-name checkout -- ruby app.rb
```

Sampling continues until the target exits, Ctrl+C, or `--duration` expires.
`--rate` controls samples per second (default 99); `--otlp-interval` controls
seconds between exports (default 60, maximum 86400). The final partial batch is
exported on normal shutdown. `--subprocesses` includes child processes and keeps
their `process.pid` resource attributes separate. Samples include `thread.id`,
Ruby function names, filenames, line numbers, and timestamps.

By default these are wall-time profiles, including time waiting. `--on-cpu`
restricts sampling to CPU activity and exports CPU profiles instead. Values and
sampling periods are expressed in nanoseconds.

With `--otlp-endpoint`, no local files are created unless `--raw-file` or `--file`
is supplied explicitly. Without it, existing file-recording behavior is unchanged.
Explicit file outputs accumulate for the full recording; omit them for indefinite
profiling. The live summary resets each export interval in OTLP mode.

## Collector configuration

The transport is OTLP/HTTP JSON over HTTP or HTTPS, using the **complete profiles
endpoint URL**, usually port 4318 and `/v1development/profiles`. It is not the gRPC
endpoint on port 4317. HTTPS uses certificate verification with public trust roots.

[otel-collector.yaml](otel-collector.yaml) provides a profiles pipeline with a debug
exporter. Run the tested Collector version from the repository root:

```sh
docker run --rm -p 127.0.0.1:4318:4318 \
  -v "$PWD/docs/otel-collector.yaml:/etc/otelcol-contrib/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.156.0 \
  --config=/etc/otelcol-contrib/config.yaml \
  --feature-gates=service.profilesSupport
```

Replace the debug exporter with a profiles-capable backend exporter for storage.
OTLP profiles have Alpha maturity; the wire package is still `v1development`.
This implementation uses development version
**1**, with schema fields from `open-telemetry/opentelemetry-proto` commit
`09769935deb8b2ebcf73ad1e2142b691d17bf70b`. It sends the
`OTLP-Profiles-Development-Version: 1` header. Older Collectors can use incompatible
schemas despite the same `v1development` URL. Collector Contrib 0.156.0 was tested
with the integration test below.

## Buffering and failures

Exports run on a separate worker so network requests do not block sampling.
Each batch is limited to 10,000 samples or approximately 4 MiB of trace data,
whichever comes first; reaching a limit triggers an early export. Two batches may
wait in the export queue. A full queue drops the new batch with a diagnostic.
Oversized individual samples are dropped with a diagnostic.

Requests time out after 10 seconds. Network errors and HTTP 429/502/503/504 get
up to two retries with backoff. Integer `Retry-After` delays up to five seconds
are honored; longer delays cause the batch to be dropped. Permanent errors and
partial rejections are not retried. Failed batches are reported and subsequent
exports continue. Normal shutdown drains the queue and can take multiple request
timeouts during an outage. The command exits unsuccessfully if any batches or
samples were lost. There is no persistent retry queue.

## Verification

The unit tests use a local HTTP server to check request headers, retries, partial
rejection, and delivery of the final batch. To run against the Collector above:

```sh
RBSPY_TEST_OTLP_ENDPOINT=http://localhost:4318/v1development/profiles \
  cargo test --lib collector_integration -- --ignored
```

The Collector debug output should contain two profiles with service name
`rbspy-integration`, PIDs 7 and 8, and the Ruby frame `work` at `app.rb:42`.

### Pyroscope integration test

The exporter is also tested through Collector Contrib 0.156.0 into Pyroscope
2.4.0. Configure the Collector's profiles pipeline with this exporter:

```yaml
exporters:
  otlp_grpc:
    endpoint: pyroscope:4040
    tls:
      insecure: true
service:
  pipelines:
    profiles:
      receivers: [otlp]
      exporters: [otlp_grpc]
```

For a disposable local Pyroscope instance, use
`grafana/pyroscope:2.4.0 -self-profiling.disable-push=true -write-path=ingester`.
The ingester path makes profiles immediately queryable; the fresh default v2
instance used in testing accepted profiles but returned empty query results.
Place the Collector and Pyroscope on the same Docker network, with `pyroscope`
as the backend's network alias, and publish their HTTP ports to localhost.

```sh
RBSPY_TEST_OTLP_ENDPOINT=http://localhost:4318/v1development/profiles \
RBSPY_TEST_PYROSCOPE_URL=http://localhost:4040 \
  cargo test --lib pyroscope_integration -- --ignored
```

This test sends synthetic Ruby stacks through the actual exporter, flushes a
batch and a final partial batch, and queries Pyroscope for both CPU and wall-time
profiles. It checks the `work` frame and an exact total of 20,000,000 nanoseconds
per profile type. It does not exercise sampling a live Ruby process.
