# esp_soulcloud_client

ESP-IDF component that connects an ESP32 to the SoulCloud platform: MQTT 5 over
WebSocket, a remote shell and request/response methods, status heartbeats, text
and on9log binary log upload, and HTTPS OTA with rollback confirmation. The wire
contract is the platform's `docs/device-protocol.md`.

Requires ESP-IDF 6.0+, `CONFIG_MQTT_PROTOCOL_5=y` and the `on9log` component.

## Use

```cpp
#include <soulcloud.hpp>

sc_config cfg = {};
cfg.pairing_token = token; // from the user's provisioning app; used only when not enrolled

auto *client = sc_client::instance();
ESP_ERROR_CHECK(client->init(cfg)); // network must be up if enrollment is needed

client->register_method("gpio.read", [](sc_rpc_call &call) {
    int64_t pin = 0;
    if (!call.params.get_int(0, "pin", pin)) {
        call.fail("params.pin must be an integer");
        return ESP_ERR_INVALID_ARG;
    }
    call.result.add_int("level", gpio_get_level(static_cast<gpio_num_t>(pin)));
    return ESP_OK;
});

client->register_shell_cmd("get sensor", "Read the sensor", [](int argc, char **argv, sc_shell_out &out) {
    out.printf("temp= %d\n", 23);
    return 0;
});

client->set_status_cb([](sc_json_writer &status) { status.begin_object("extra").add_int("temp", 23).end_object(); });

client->start();
```

Register methods, shell commands and callbacks between `init()` and `start()`.

## Built in

- Methods: `shell`, `reboot`, `info`, `log.config`, `ota.start`, `ota.cancel`.
- Shell: `help`, `status uptime`, `get app_desc`, `get elf_sha256`, `get unique_id`,
  `get chip`, `get free_heap [internal|external]`, `get partitions`,
  `get reset_reason`, `get time`, `dns_resolve <host>`, `reset main_device`.
- After connecting: a birth status, a heartbeat every
  `CONFIG_SOULCLOUD_HEARTBEAT_INTERVAL_SEC`, the clock from the platform if
  unset, an `ota.check`, and confirmation of a freshly updated image.

## Memory

All buffers are allocated once by `init()` (sizes in menuconfig, "SoulCloud
client"): request slots, a response buffer, two log ring buffers and a log batch
buffer (PSRAM when available). Tasks: the request task, the log upload task, and
an OTA task that exists only during an update. The JSON writer and reader never
allocate.

## Layout

| File | Role |
|---|---|
| `include/soulcloud.hpp` | `sc_client` facade and handler types |
| `include/sc_json.hpp` | JSON writer and reader (jsmn) |
| `src/sc_mqtt` | esp-mqtt connection, topic routing, publishing |
| `src/sc_rpc` | request slots, method table, calls to the platform |
| `src/sc_codec` | encoding of the fixed messages (JSON) |
| `src/sc_credentials` | NVS identity and enrollment |
| `src/sc_shell` | shell command table and built-ins |
| `src/sc_status` | birth and heartbeat reports |
| `src/sc_log_uplink` | on9log sink, ESP_LOG hook, upload task |
| `src/sc_ota` | HTTPS download, SHA-256 check, Range resume |

Host test for the JSON code: `test/host/json_test.cpp` (build command at its top).
