# MQTT Device Profile validation

This page records one local run on 2026-09-25, on macOS arm64, with the debug
`device_mqtt` example. Results describe this run only; they are not production
capacity figures. Reproduce with:

```sh
cargo build --locked -p netbaiot-device-sdk --example device_mqtt
python3 tests/run_device_profile_mosquitto.py
python3 tests/measure_device_profile.py
```

The Mosquitto script uses a separate external broker for MQTT 3.1.1 and MQTT 5,
plain TCP and verified TLS. It checks rejection of untrusted, wrong-host, and
expired certificates. It also restarts a persistent broker five times for each
MQTT version. The same SDK process used 4,624, 4,672, 4,672, 4,672, 4,672 KiB
RSS before successive 3.1.1 restarts, and 4,624, 4,672, 4,688, 4,688,
4,688 KiB before successive 5.0 restarts. Neither series
grew after the third sample. This is a short recovery check, not a soak test.

With MQTT 3.1.1 over local TCP, 200 sequential QoS1 telemetry publishes and
PUBACK receipts completed at 10,423.8/s; observed publish-to-PUBACK latency was
p50 88 µs, p95 141 µs, p99 204 µs. This includes the client and local Mosquitto
on the same host, but not device networking or NetbaIoT business sinks.

The independent fake broker deliberately withheld PUBACK. One idle SDK process
used 4,432 KiB RSS, 11 OS threads, and one live Tokio task. At the configured
count/byte limits it
accepted 32 simultaneous 60,000-character telemetry messages before reporting
`Overloaded`; its RSS was 8,064 KiB with 11 OS threads and one live Tokio task.
The thread count includes the Tokio runtime and process overhead. The live
Tokio task count comes from `Handle::metrics()` in this single-client probe.

The full workspace tests, the 31-case NetbaIoT MQTT conformance suite, and
10,000 libFuzzer runs of the new client decoder also passed in this run. The
standalone official-client integration test separately measured 200 gateway
events; its throughput and RSS include management and gateway components, so
they should not be compared directly with the Device Profile probe above.
