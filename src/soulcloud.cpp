/*
 * Soulcloud device client core: lifecycle, MQTT event orchestration,
 * periodic stat reports and command dispatch wiring.
 */

#include "soulcloud.hpp"

#include <cstdio>
#include <cstring>

#include <esp_app_desc.h>
#include <esp_log.h>
#include <esp_timer.h>
#include <freertos/FreeRTOS.h>
#include <freertos/ringbuf.h>
#include <freertos/task.h>
#include <soc/soc_caps.h>

#include <esp_heap_caps.h>

#include "commands.hpp"
#include "logs.hpp"
#include "mqtt_bridge.hpp"
#include "ota.hpp"
#include "protocol.hpp"

using namespace soulcloud;

const char *soulcloud::soulcloud_client::reset_reason_str(esp_reset_reason_t r)
{
    switch (r) {
    case ESP_RST_UNKNOWN: return "UNKNOWN";
    case ESP_RST_POWERON: return "POWERON";
    case ESP_RST_EXT: return "EXT";
    case ESP_RST_SW: return "SW";
    case ESP_RST_PANIC: return "PANIC";
    case ESP_RST_INT_WDT: return "INT_WDT";
    case ESP_RST_TASK_WDT: return "TASK_WDT";
    case ESP_RST_WDT: return "WDT";
    case ESP_RST_DEEPSLEEP: return "DEEPSLEEP";
    case ESP_RST_BROWNOUT: return "BROWNOUT";
    case ESP_RST_SDIO: return "SDIO";
    case ESP_RST_USB: return "USB";
    case ESP_RST_JTAG: return "JTAG";
    case ESP_RST_EFUSE: return "EFUSE";
    case ESP_RST_PWR_GLITCH: return "PWR_GLITCH";
    case ESP_RST_CPU_LOCKUP: return "CPU_LOCKUP";
    default: return "UNKNOWN";
    }
}

int soulcloud::soulcloud_client::hex_val(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return 0;
}

void soulcloud::soulcloud_client::hex_to_bin(const char *hex, uint8_t *bin, size_t len)
{
    for (size_t i = 0; i < len; ++i) {
        bin[i] = (uint8_t)((hex_val(hex[i * 2]) << 4) | hex_val(hex[i * 2 + 1]));
    }
}

bool soulcloud::soulcloud_client::topic_matches(const char *topic, size_t topic_len, const char *expected)
{
    const size_t n = strlen(expected);
    return topic_len == n && memcmp(topic, expected, n) == 0;
}

// ------------------------------------------------------------------ //
// PIMPL
// ------------------------------------------------------------------ //

class soulcloud::soulcloud_client::client_impl
{
public:
    explicit client_impl(soulcloud_client &owner) : owner(owner) {}

    mqtt_bridge bridge;
    esp_timer_handle_t stat_timer = nullptr;
    soulcloud_client &owner;

    // Dedicated inbound dispatch task: command handlers and OTA notice
    // handling are application-ish code with unpredictable stack needs;
    // they must not run on the esp-mqtt (6144 B) or esp_timer (3584 B)
    // task stacks. The MQTT event callback only assembles a small item
    // (header + payload copy) into a FreeRTOS ring buffer and never runs
    // handlers itself; the core task drains the buffer.
    volatile TaskHandle_t task = nullptr;
    RingbufHandle_t inbound_rb = nullptr;
    volatile bool exit = false;

    enum inbound_kind : uint8_t {
        INBOUND_NONE = 0,
        INBOUND_CMD,  // cmd/exec payload
        INBOUND_OTA,  // ota notice payload
    };

    /** Ring buffer item header (4 bytes, aligned). */
    struct inbound_header
    {
        uint8_t kind;   // inbound_kind
        uint8_t pad;    // keep len 16-bit aligned
        uint16_t len;   // payload length
    };

    // C-callback trampolines
    static void mqtt_on_connected(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner.on_mqtt_connected();
    }

    static void mqtt_on_disconnected(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner.on_mqtt_disconnected();
    }

    static void mqtt_on_data(void *ctx, const char *topic, size_t topic_len,
                             const uint8_t *data, size_t data_len)
    {
        static_cast<client_impl *>(ctx)->owner.on_mqtt_data(topic, topic_len, data, data_len);
    }

    static void stat_timer_cb(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner.report_stat();
    }

    static void task_main(void *ctx)
    {
        static_cast<client_impl *>(ctx)->run();
    }

    void run()
    {
        for (;;) {
            // Ring buffers are polled with zero-tick receives (empty is a
            // NULL return), so no wake-up signalling is needed; a 1-tick
            // delay keeps the poll cheap and bounds latency.

            // drain inbound (commands/OTA) without blocking
            size_t len = 0;
            uint8_t *item = (uint8_t *)xRingbufferReceive(inbound_rb, &len, 0);
            while (item != nullptr) {
                if (len >= sizeof(inbound_header)) {
                    inbound_header hdr = {};
                    memcpy(&hdr, item, sizeof(hdr));  // unaligned-safe read
                    const uint8_t *payload = item + sizeof(hdr);
                    const size_t plen = hdr.len;
                    if (plen <= len - sizeof(hdr)) {  // truncated-item guard
                        switch (hdr.kind) {
                        case INBOUND_CMD:
                            owner.dispatch_command(payload, plen);
                            break;
                        case INBOUND_OTA:
                            owner.handle_ota_notice(payload, plen);
                            break;
                        default:
                            ESP_LOGW(TAG, "unknown inbound kind %u", (unsigned)hdr.kind);
                            break;
                        }
                    }
                }
                vRingbufferReturnItem(inbound_rb, item);
                item = (uint8_t *)xRingbufferReceive(inbound_rb, &len, 0);
            }

            // drain queued log packets (throttled publish, never blocks)
            soulcloud::log_sender::instance().drain();

            if (exit) {
                break;
            }
            vTaskDelay(1);  // 1 RTOS tick poll interval
        }
        task = nullptr;  // tell deinit() we are gone (before self-delete)
        vTaskDelete(nullptr);
    }
};

// ------------------------------------------------------------------ //
// lifecycle
// ------------------------------------------------------------------ //

esp_err_t soulcloud::soulcloud_client::init(const config *cfg)
{
    if (inited) {
        return ESP_ERR_INVALID_STATE;
    }
    if (cfg == nullptr || cfg->device_uid[0] == '\0' || cfg->broker_uri[0] == '\0') {
        return ESP_ERR_INVALID_ARG;
    }

    _cfg = *cfg;
    impl = new client_impl(*this);  // sole heap allocation (deinit frees)

    const mqtt_callbacks cbs = {
        .on_connected = client_impl::mqtt_on_connected,
        .on_disconnected = client_impl::mqtt_on_disconnected,
        .on_data = client_impl::mqtt_on_data,
        .on_error = nullptr,
        .ctx = impl,
    };

    esp_err_t err = impl->bridge.init(cfg, &cbs);
    if (err != ESP_OK) {
        delete impl;
        impl = nullptr;
        return err;
    }

    const esp_timer_create_args_t timer_args = {
        .callback = client_impl::stat_timer_cb,
        .arg = impl,
        .dispatch_method = ESP_TIMER_TASK,
        .name = "soulcloud_stat",
        .skip_unhandled_events = false,
    };
    err = esp_timer_create(&timer_args, &impl->stat_timer);
    if (err != ESP_OK) {
        impl->bridge.deinit();
        delete impl;
        impl = nullptr;
        return err;
    }

    // Dedicated inbound dispatch task: handlers are application code with
    // unpredictable stack needs, so they run here (large, PSRAM-backed
    // stack) instead of on the esp-mqtt/esp_timer task stacks. Inbound
    // messages queue up in a FreeRTOS ring buffer (bytebuf), so a burst
    // of commands/notices is drained in order instead of dropped.
    impl->inbound_rb = xRingbufferCreateWithCaps(CONFIG_SOULCLOUD_INBOUND_RB_SIZE,
                                                   RINGBUF_TYPE_BYTEBUF,
                                                   MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (impl->inbound_rb == nullptr) {
        esp_timer_delete(impl->stat_timer);
        impl->bridge.deinit();
        delete impl;
        impl = nullptr;
        return ESP_ERR_NO_MEM;
    }
    TaskHandle_t task = nullptr;
    err = xTaskCreateWithCaps(client_impl::task_main, "soulcloud_core",
                              CONFIG_SOULCLOUD_CORE_TASK_STACK, impl,
                              5, &task, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    impl->task = task;
    if (err != pdPASS) {
        vRingbufferDelete(impl->inbound_rb);
        impl->inbound_rb = nullptr;
        esp_timer_delete(impl->stat_timer);
        impl->bridge.deinit();
        delete impl;
        impl = nullptr;
        return ESP_ERR_NO_MEM;
    }

    // NOTE: pass the copy (cfg) not the caller's stack pointer: log_sender
    // and ota_executor keep the pointer for their whole lifetime, and the
    // caller's `cfg` lives on the main_task stack only until init() returns.
    err = soulcloud::log_sender::instance().init(&_cfg, &impl->bridge);
    if (err != ESP_OK) {
        esp_timer_delete(impl->stat_timer);
        impl->bridge.deinit();
        delete impl;
        impl = nullptr;
        return err;
    }

    err = soulcloud::ota_executor::instance().init(&_cfg, &impl->bridge);
    if (err != ESP_OK) {
        soulcloud::log_sender::instance().deinit();
        esp_timer_delete(impl->stat_timer);
        impl->bridge.deinit();
        delete impl;
        impl = nullptr;
        return err;
    }

    inited = true;
    ESP_LOGI(TAG, "initialised (uid=%s, broker=%s)", _cfg.device_uid, _cfg.broker_uri);
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::start()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (started) {
        return ESP_OK;
    }
    esp_err_t err = impl->bridge.start();
    if (err != ESP_OK) {
        return err;
    }
    started = true;
    if (impl->stat_timer != nullptr) {
        esp_timer_start_periodic(impl->stat_timer, (uint64_t)_cfg.stat_interval_s * 1000000ull);
    }
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::stop()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (impl->stat_timer != nullptr) {
        esp_timer_stop(impl->stat_timer);
    }
    impl->bridge.stop();
    started = false;
    connected = false;
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::deinit()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (impl->stat_timer != nullptr) {
        esp_timer_stop(impl->stat_timer);
        esp_timer_delete(impl->stat_timer);
    }
    // stop the core task: flag + signal, then wait for it to exit
    if (impl->task != nullptr) {
        impl->exit = true;
        // the task polls the ring buffer on a short timeout, so it wakes
        // up and self-deletes; wait briefly for it to finish
        for (uint32_t i = 0; i < 100 && impl->task != nullptr; ++i) {
            vTaskDelay(pdMS_TO_TICKS(10));
        }
        if (impl->task != nullptr) {
            vTaskDelete(impl->task);  // forced (should not happen)
        }
        impl->task = nullptr;
    }
    if (impl->inbound_rb != nullptr) {
        vRingbufferDelete(impl->inbound_rb);
        impl->inbound_rb = nullptr;
    }
    soulcloud::log_sender::instance().deinit();
    soulcloud::ota_executor::instance().deinit();
    impl->bridge.deinit();
    delete impl;
    impl = nullptr;
    inited = false;
    started = false;
    connected = false;
    return ESP_OK;
}

// ------------------------------------------------------------------ //
// commands
// ------------------------------------------------------------------ //

esp_err_t soulcloud::soulcloud_client::register_command(const char *name, command_handler_t handler, void *ctx)
{
    return soulcloud::command_registry::instance().register_command(name, handler, ctx);
}

// ------------------------------------------------------------------ //
// stat
// ------------------------------------------------------------------ //

void soulcloud::soulcloud_client::build_stat(uint8_t *buf, size_t cap, size_t *out_len)
{
    char fw_hex[65] = {};
    esp_app_get_elf_sha256(fw_hex, sizeof(fw_hex));
    uint8_t fw[FW_SHA256_LEN] = {};
    hex_to_bin(fw_hex, fw, FW_SHA256_LEN);

    const char *rst = reset_reason_str(esp_reset_reason());
    const uint64_t up = (uint64_t)(esp_timer_get_time() / 1000000);

    const device_stat stat = {
        .sn = _cfg.serial,
        .sn_len = strlen(_cfg.serial),
        .fw = fw,
        .fw_len = FW_SHA256_LEN,
        .up = up,
        .rst = rst,
    };
    encode_stat(buf, cap, out_len, &stat);
}

esp_err_t soulcloud::soulcloud_client::report_stat()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    uint8_t buf[256] = {};
    size_t len = 0;
    build_stat(buf, sizeof(buf), &len);

    char topic[160] = {};
    topic_stat(topic, sizeof(topic), _cfg.device_uid);
    const int32_t msg_id = impl->bridge.publish(topic, buf, len, 1);
    if (msg_id < 0) {
        ESP_LOGW(TAG, "stat publish failed");
        return ESP_FAIL;
    }
    ESP_LOGD(TAG, "stat published (%u bytes)", (unsigned)len);
    return ESP_OK;
}

// ------------------------------------------------------------------ //
// MQTT event handling
// ------------------------------------------------------------------ //

void soulcloud::soulcloud_client::on_mqtt_connected()
{
    connected = true;

    char topic[160] = {};
    topic_cmd_exec(topic, sizeof(topic), _cfg.device_uid);
    impl->bridge.subscribe(topic, 1);
    topic_ota(topic, sizeof(topic), _cfg.device_uid);
    impl->bridge.subscribe(topic, 1);

    ESP_LOGI(TAG, "connected; subscribed to cmd/exec and ota");
    report_stat();

    if (conn_cb != nullptr) {
        conn_cb(true, conn_ctx);
    }
}

void soulcloud::soulcloud_client::on_mqtt_disconnected()
{
    connected = false;
    if (conn_cb != nullptr) {
        conn_cb(false, conn_ctx);
    }
}

void soulcloud::soulcloud_client::on_mqtt_data(const char *topic, size_t topic_len,
                                    const uint8_t *data, size_t data_len)
{
    if (impl == nullptr || impl->inbound_rb == nullptr) {
        return;
    }

    // Classify the topic (cheap) and enqueue header + payload copy into
    // the core task's ring buffer. Handlers run on the dedicated core
    // task: they are application code and must not execute on the small
    // esp-mqtt event task stack.
    char expected[160] = {};

    client_impl::inbound_kind kind = client_impl::INBOUND_NONE;
    topic_cmd_exec(expected, sizeof(expected), _cfg.device_uid);
    if (topic_matches(topic, topic_len, expected)) {
        kind = client_impl::INBOUND_CMD;
    } else {
        topic_ota(expected, sizeof(expected), _cfg.device_uid);
        if (topic_matches(topic, topic_len, expected)) {
            kind = client_impl::INBOUND_OTA;
        }
    }
    if (kind == client_impl::INBOUND_NONE) {
        ESP_LOGW(TAG, "message on unexpected topic %.*s", (int)topic_len, topic);
        return;
    }

    if (data_len > CONFIG_SOULCLOUD_INBOUND_MAX) {
        ESP_LOGW(TAG, "inbound payload too large (%u bytes); dropping", (unsigned)data_len);
        return;
    }

    // Assemble one ring buffer item: [header][payload]. The header lives
    // on this stack; the payload is copied in. Commands/notices are
    // low-rate, so the short-lived PSRAM allocation is acceptable (the
    // ring buffer copies the bytes, then we free the staging buffer).
    client_impl::inbound_header hdr = {};
    hdr.kind = (uint8_t)kind;
    hdr.len = (uint16_t)data_len;
    const size_t item_len = sizeof(hdr) + data_len;
    uint8_t *item = (uint8_t *)heap_caps_malloc(item_len, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (item == nullptr) {
        ESP_LOGW(TAG, "no memory for inbound staging buffer; dropping");
        return;
    }
    memcpy(item, &hdr, sizeof(hdr));
    memcpy(item + sizeof(hdr), data, data_len);

    // bounded wait for a free slot (backpressure); drop when full
    // (QoS1 redelivery covers commands, the server re-issues OTA notices)
    if (xRingbufferSend(impl->inbound_rb, item, item_len, pdMS_TO_TICKS(100)) != pdTRUE) {
        ESP_LOGW(TAG, "inbound ring buffer full; dropping %u-byte message", (unsigned)data_len);
    }
    heap_caps_free(item);
}

void soulcloud::soulcloud_client::dispatch_command(const uint8_t *payload, size_t len)
{
    // commands are paused during OTA
    if (soulcloud::ota_executor::instance().is_active()) {
        ESP_LOGD(TAG, "command ignored: OTA in progress");
        return;
    }
    uint8_t result_buf[1024] = {};
    const int32_t n = soulcloud::command_registry::instance().dispatch(payload, len,
                                                            result_buf, sizeof(result_buf));
    if (n > 0) {
        char topic[160] = {};
        topic_cmd_result(topic, sizeof(topic), _cfg.device_uid);
        impl->bridge.publish(topic, result_buf, (size_t)n, 1);
    } else {
        ESP_LOGW(TAG, "cmd/exec dispatch failed (%ld)", (long)n);
    }
}

void soulcloud::soulcloud_client::handle_ota_notice(const uint8_t *payload, size_t len)
{
    ota_notice notice;
    const int32_t rc = decode_ota_notice(payload, len, &notice);
    if (rc != ERR_OK) {
        ESP_LOGW(TAG, "decode ota notice failed (%ld)", (long)rc);
        return;
    }
    const esp_err_t err = soulcloud::ota_executor::instance().start(&notice);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "ota start failed: %s", esp_err_to_name(err));
    }
}

void soulcloud::soulcloud_client::notify_wifi_connected()
{
    if (impl != nullptr) {
        impl->bridge.notify_wifi_connected();
    }
}
