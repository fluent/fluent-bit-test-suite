# Fluent Bit Python Integration Test Suite

## What This Is

This is a binary-level integration test harness for Fluent Bit.

This project is distributed under the Apache License, Version 2.0.

It starts a real Fluent Bit process, drives real network traffic into it, captures what Fluent Bit emits, and asserts on observable behavior:

- listener behavior
- protocol negotiation
- payload parsing
- payload transformation
- downstream request generation
- exporter endpoint output
- negative-path handling
- memory diagnostics through optional Valgrind execution

The suite is designed as a reusable tool for testing Fluent Bit plugins and protocol behavior with deterministic local infrastructure.

## What This Solves

This framework gives you a controlled environment for testing Fluent Bit end to end without depending on external services.

It provides:

- dynamic port allocation
- local fake HTTP servers
- local fake OTLP receivers over HTTP and gRPC
- TLS-enabled endpoints with reusable local certificates
- HTTP/1.1 and HTTP/2 matrix execution
- bounded waits instead of ad hoc sleeps
- per-run logs and result directories
- optional Valgrind wrapping and parsing

That makes it useful both for plugin development and for runtime regression testing.

## High-Level Architecture

```text
                    +----------------------+
                    |   Python test case   |
                    |      (pytest)        |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | FluentBitTestService |
                    | port/env orchestration
                    | server lifecycle
                    | bounded waits
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  FluentBitManager    |
                    | start/stop binary
                    | logs/results
                    | readiness
                    | valgrind integration
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |   fluent-bit binary  |
                    +-----+----------+-----+
                          |          |
              input side  |          |  output side
                          v          v
              real clients/tests   fake receivers/exporters
```

## Main Components

### Process management

[`src/utils/fluent_bit_manager.py`](src/utils/fluent_bit_manager.py)

- resolves the Fluent Bit binary
- starts Fluent Bit with a scenario config
- captures logs in a per-run results directory
- exposes monitoring-based readiness checks
- supports `VALGRIND=1`

[`src/utils/test_service.py`](src/utils/test_service.py)

- allocates dynamic ports
- injects environment variables into scenario configs
- starts and stops helper servers
- exposes deterministic wait helpers for assertions

### Transport matrix

[`src/utils/http_matrix.py`](src/utils/http_matrix.py)

- drives HTTP/1.1 cleartext
- drives HTTP/2 cleartext
- drives HTTP/1.1 over TLS
- drives HTTP/2 over TLS
- exercises upgrade and fallback behavior when relevant

### Helper servers

[`src/server/http_server.py`](src/server/http_server.py)

- fake HTTP destination
- captures request headers, body, path, auth, and metadata
- can inject response codes, delays, OAuth token responses, and JWKS responses

[`src/server/otlp_server.py`](src/server/otlp_server.py)

- fake OTLP receiver
- accepts OTLP over HTTP and gRPC
- decodes protobuf payloads
- supports gzip and zstd request decoding
- captures method/path/headers/transport for assertions

[`src/server/splunk_server.py`](src/server/splunk_server.py)

- Splunk-oriented downstream capture helper

## Data Flow

### Input plugin tests

```text
test client
   |
   | real protocol payload
   v
+------------------+
|  Fluent Bit      |
|  input plugin    |
+------------------+
   |
   | forwarded output
   v
+------------------+
| fake receiver    |
| http / otlp      |
+------------------+
   |
   v
pytest assertions
```

Examples:

- `in_http`
- `in_splunk`
- `in_elasticsearch`
- `in_opentelemetry`
- `in_syslog`

### Output plugin tests

```text
source input inside Fluent Bit
dummy / metrics / otlp / etc
          |
          v
+------------------+
|  Fluent Bit      |
|  output plugin   |
+------------------+
   |
   | outbound request
   v
+------------------+
| fake receiver    |
| http / otlp      |
+------------------+
   |
   v
pytest assertions
```

Examples:

- `out_http`
- `out_opentelemetry`
- `out_prometheus_exporter`
- `out_vivo_exporter`

### Internal endpoint tests

```text
pytest client
   |
   v
+------------------+
| Fluent Bit       |
| internal server  |
+------------------+
   |
   v
response validation
```

Examples:

- `internal_http_server`
- `out_prometheus_exporter`
- `out_vivo_exporter`

## TLS And Protocol Matrix

The suite reuses a local certificate pair from:

- [`scenarios/in_splunk/certificate/certificate.pem`](scenarios/in_splunk/certificate/certificate.pem)
- [`scenarios/in_splunk/certificate/private_key.pem`](scenarios/in_splunk/certificate/private_key.pem)

These assets are shared across HTTP and OTLP TLS scenarios.

The HTTP matrix covers, depending on plugin support:

- HTTP/1.1 cleartext
- HTTP/2 cleartext with prior knowledge
- cleartext upgrade attempts
- fallback to HTTP/1.1
- HTTP/1.1 over TLS
- HTTP/2 over TLS via ALPN
- HTTP/2 TLS fallback to HTTP/1.1

This lets the suite validate not only payload handling, but also the transport behavior exposed by Fluent Bit listeners and endpoints.

## Current Coverage

The suite currently exercises:

- HTTP input plugins
- OTLP input and output paths
- Splunk HEC input behavior
- Elasticsearch-compatible input behavior
- syslog ingestion over TCP, TLS, UDP, and Unix sockets
- Prometheus remote write ingestion
- Prometheus and Vivo exporters
- internal HTTP server endpoints
- connection limiting behavior
- OAuth2 and JWT flows for supported plugins
- compression with gzip and zstd
- selected end-to-end plugin-to-plugin behavior

## Scenario Index

### `in_http`

Path: [`scenarios/in_http`](scenarios/in_http)

Entry point: [`scenarios/in_http/tests/test_in_http_001.py`](scenarios/in_http/tests/test_in_http_001.py)

Covers:

- request acceptance and downstream forwarding
- HTTP transport matrix
- malformed JSON rejection
- invalid method rejection
- JWT/OAuth2 protected input behavior

### `in_elasticsearch`

Path: [`scenarios/in_elasticsearch`](scenarios/in_elasticsearch)

Entry point: [`scenarios/in_elasticsearch/tests/test_in_elasticsearch_001.py`](scenarios/in_elasticsearch/tests/test_in_elasticsearch_001.py)

Covers:

- root and `/_nodes/http` endpoints
- bulk create, update, and delete operations
- HTTP transport matrix
- invalid bulk request handling

### `in_opentelemetry`

Path: [`scenarios/in_opentelemetry`](scenarios/in_opentelemetry)

Entry point: [`scenarios/in_opentelemetry/tests/test_in_opentelemetry_001.py`](scenarios/in_opentelemetry/tests/test_in_opentelemetry_001.py)

Covers:

- OTLP logs, metrics, and traces ingestion
- semantic validation of re-emitted OTLP
- histogram and gauge metrics
- parent/child traces
- invalid payload handling
- receiver error visibility
- HTTP transport matrix

### `in_splunk`

Path: [`scenarios/in_splunk`](scenarios/in_splunk)

Entry point: [`scenarios/in_splunk/tests/test_in_splunk_001.py`](scenarios/in_splunk/tests/test_in_splunk_001.py)

Covers:

- Splunk HEC URI variants
- HTTP transport matrix
- keepalive variations
- invalid request handling
- output-token precedence in `in_splunk -> out_splunk`

### `in_prometheus_remote_write`

Path: [`scenarios/in_prometheus_remote_write`](scenarios/in_prometheus_remote_write)

Entry point: [`scenarios/in_prometheus_remote_write/tests/test_in_prometheus_remote_write_001.py`](scenarios/in_prometheus_remote_write/tests/test_in_prometheus_remote_write_001.py)

Covers:

- remote-write ingestion using a real Fluent Bit sender
- HTTP/1 and HTTP/2 receiver modes
- cleartext and TLS receiver modes

### `in_http_max_connections`

Path: [`scenarios/in_http_max_connections`](scenarios/in_http_max_connections)

Entry point: [`scenarios/in_http_max_connections/tests/test_in_http_max_connections_001.py`](scenarios/in_http_max_connections/tests/test_in_http_max_connections_001.py)

Covers:

- `http_server.max_connections`
- deterministic block and recovery behavior

### `in_syslog`

Path: [`scenarios/in_syslog`](scenarios/in_syslog)

Entry point: [`scenarios/in_syslog/tests/test_in_syslog_001.py`](scenarios/in_syslog/tests/test_in_syslog_001.py)

Covers:

- TCP plaintext
- TCP TLS
- UDP plaintext
- Unix stream sockets
- Unix datagram sockets

### `internal_http_server`

Path: [`scenarios/internal_http_server`](scenarios/internal_http_server)

Entry point: [`scenarios/internal_http_server/tests/test_internal_http_server_001.py`](scenarios/internal_http_server/tests/test_internal_http_server_001.py)

Covers:

- representative internal endpoints
- response headers
- concurrency behavior
- selected HTTP/2 access

### `out_http`

Path: [`scenarios/out_http`](scenarios/out_http)

Entry point: [`scenarios/out_http/tests/test_out_http_001.py`](scenarios/out_http/tests/test_out_http_001.py)

Covers:

- outbound JSON delivery
- receiver error observability
- OAuth2 client credentials
- OAuth2 private key JWT

### `out_opentelemetry`

Path: [`scenarios/out_opentelemetry`](scenarios/out_opentelemetry)

Entry point: [`scenarios/out_opentelemetry/tests/test_out_opentelemetry_001.py`](scenarios/out_opentelemetry/tests/test_out_opentelemetry_001.py)

Covers:

- OTLP logs, metrics, and traces output
- HTTP and gRPC transport
- custom HTTP and gRPC URIs
- TLS verification and vhost/SNI behavior
- custom headers and basic auth
- gzip and zstd compression
- `logs_body_key`
- `logs_body_key_attributes`
- metadata and message key mapping
- `add_label`
- `batch_size`
- `logs_max_resources`
- `logs_max_scopes`

### `out_prometheus_exporter`

Path: [`scenarios/out_prometheus_exporter`](scenarios/out_prometheus_exporter)

Entry point: [`scenarios/out_prometheus_exporter/tests/test_out_prometheus_exporter_001.py`](scenarios/out_prometheus_exporter/tests/test_out_prometheus_exporter_001.py)

Covers:

- scrapeable `/metrics`
- selected HTTP/2 access

### `out_vivo_exporter`

Path: [`scenarios/out_vivo_exporter`](scenarios/out_vivo_exporter)

Entry point: [`scenarios/out_vivo_exporter/tests/test_out_vivo_exporter_001.py`](scenarios/out_vivo_exporter/tests/test_out_vivo_exporter_001.py)

Covers:

- exporter endpoints
- headers
- selected HTTP/2 access

## Running

Run the full suite:

```bash
./tests/fluent-bit-test-suite/.venv/bin/pytest -q tests/fluent-bit-test-suite
```

Run against a different binary:

```bash
FLUENT_BIT_BINARY=/path/to/fluent-bit \
./tests/fluent-bit-test-suite/.venv/bin/pytest -q tests/fluent-bit-test-suite
```

Run under Valgrind:

```bash
VALGRIND=1 ./tests/fluent-bit-test-suite/.venv/bin/pytest -q tests/fluent-bit-test-suite
```

Require Valgrind-clean runs:

```bash
VALGRIND=1 VALGRIND_STRICT=1 \
./tests/fluent-bit-test-suite/.venv/bin/pytest -q tests/fluent-bit-test-suite
```
