# `netbaiot` CLI

The CLI is an operator/debug tool and a dogfood client for `netbaiot-client`. It
does not implement HTTP or stream protocols itself.

Configuration uses `--endpoint`, `--token`, `--event-address`, `--event-token`, and
`--output human|json`, or `NETBAIOT_ENDPOINT`, `NETBAIOT_TOKEN`,
`NETBAIOT_EVENT_ADDRESS`, and `NETBAIOT_EVENT_TOKEN`. Device commands additionally
accept `NETBAIOT_TENANT` and `NETBAIOT_PRODUCT`. Tokens are never printed.

Implemented commands:

```text
netbaiot server status
netbaiot server drain --yes
netbaiot device status DEVICE --tenant TENANT --product PRODUCT
netbaiot events subscribe [--tenant ...] [--product ...] [--device ...] [--type ...]
netbaiot command send DEVICE --json JSON
netbaiot command send DEVICE --payload-file PATH
netbaiot config get DEVICE
netbaiot config set DEVICE --file PATH --revision N
netbaiot auth invalidate --device DEVICE
```

Event output is flushed before manual ACK. `--output json` emits only JSON/JSONL on
stdout. Drain requires `--yes`. Exit codes are 0 success, 2 usage, 3 authentication,
4 forbidden, 5 device offline, and 6 unavailable/other runtime failure.
