# Contributing to Fluent Bit Test Suite

> note: this guide is still work in process

The way to contribute to this project is through the official Github Repository  [fluent/fluent-bit-test-suite](https://github.com/fluent/fluent-bit-test-suite). All contributions must adhere to this guidelines that aims to make easier it maintenance and high quality over time.

## Why contributing to this project ?

Fluent Bit is one of the widely deployed Telemetry Agent around the globe, millions of new deployments happens every single day. Contributing to this project aims to extend the testing of different areas under different complex configurations.

Our goal is to avoid regressions and making sure Fluent Bit can continue to grow in a healthy way.

## Guidelines

All the code in this project is based on Python 3.x and we have a few requirements:

### Code Style

Avoid using camelCase in variables, functions and method names, use the underscore (`_`) instead.

### GIT Commits

In open source project maintenance, having a clear history is key for us, for hence we expect full clarity in the commits. Clarity must be in the following places:

- commit prefix
- commit description

#### Commit Prefix

The project have specific components, we enforce that every commit that touches an interface or component be prefixed with that name. As of today we register the following components:

__scenarios__

An scenario defines a main type of pipeline or Fluent Bit component being tested, an example is:

- otlp-otlp: changes applicable to otlp-to-otlp tests

For any code change that is happening inside [scenarios/otlp-to-otlp](https://github.com/fluent/fluent-bit-test-suite/tree/main/scenarios/otlp-to-otlp) the commit must be prefixed like this:

```
scenarios: otlp-to-otlp: description of the change
```

Optionally you can add a third component that represents another file interface (without the .py.

__server__

In the server components, we have helpers to implement 'fake servers' who mimic other projects that we use as receivers, e.g:

| server | description |
|--|--|
| [http](https://github.com/fluent/fluent-bit-test-suite/blob/main/src/server/http_server.py) | Simple HTTP server |
| [otlp](https://github.com/fluent/fluent-bit-test-suite/blob/main/src/server/otlp_server.py) | OpenTelemetry HTTP Server |
| [splunk](https://github.com/fluent/fluent-bit-test-suite/blob/main/src/server/splunk_server.py) | Splunk HTTP Server |

When modifying any of those servers or adding new ones, the commits must be prefixed like this:

```
server: http: some example descripition
```

##### Others

Commits should not modify files outside of the scope defined in the prefix, while there might be cases for exceptions we will handle those in the Pull Request review process.


### License

All code contributed to this project is under the terms of the Apache v2 License. All commits must be signed (DCO).
