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
    explicit client_impl(soulcloud_client &owner) : owner_(owner) {}

    mqtt_bridge bridge;
    esp_timer_handle_t stat_timer = nullptr;
    soulcloud_client &owner_;

    // C-callback trampolines
    static void mqtt_on_connected(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner_.on_mqtt_connected();
    }

    static void mqtt_on_disconnected(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner_.on_mqtt_disconnected();
    }

    static void mqtt_on_data(void *ctx, const char *topic, size_t topic_len,
                             const uint8_t *data, size_t data_len)
    {
        static_cast<client_impl *>(ctx)->owner_.on_mqtt_data(topic, topic_len, data, data_len);
    }

    static void stat_timer_cb(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner_.report_stat();
    }
};

// ------------------------------------------------------------------ //
// lifecycle
// ------------------------------------------------------------------ //

esp_err_t soulcloud::soulcloud_client::init(const config *cfg)
{
    if (inited_) {
        return ESP_ERR_INVALID_STATE;
    }
    if (cfg == nullptr || cfg->device_uid[0] == '\0' || cfg->broker_uri[0] == '\0') {
        return ESP_ERR_INVALID_ARG;
    }

    cfg_ = *cfg;
    impl_ = new client_impl(*this);  // sole heap allocation (deinit frees)

    const mqtt_callbacks cbs = {
        .on_connected = client_impl::mqtt_on_connected,
        .on_disconnected = client_impl::mqtt_on_disconnected,
        .on_data = client_impl::mqtt_on_data,
        .on_error = nullptr,
        .ctx = impl_,
    };

    esp_err_t err = impl_->bridge.init(cfg, &cbs);
    if (err != ESP_OK) {
        delete impl_;
        impl_ = nullptr;
        return err;
    }

    const esp_timer_create_args_t timer_args = {
        .callback = client_impl::stat_timer_cb,
        .arg = impl_,
        .dispatch_method = ESP_TIMER_TASK,
        .name = "soulcloud_stat",
        .skip_unhandled_events = false,
    };
    err = esp_timer_create(&timer_args, &impl_->stat_timer);
    if (err != ESP_OK) {
        impl_->bridge.deinit();
        delete impl_;
        impl_ = nullptr;
        return err;
    }

    // NOTE: pass the copy (cfg_) not the caller's stack pointer: log_sender
    // and ota_executor keep the pointer for their whole lifetime, and the
    // caller's `cfg` lives on the main_task stack only until init() returns.
    err = soulcloud::log_sender::instance().init(&cfg_, &impl_->bridge);
    if (err != ESP_OK) {
        esp_timer_delete(impl_->stat_timer);
        impl_->bridge.deinit();
        delete impl_;
        impl_ = nullptr;
        return err;
    }

    err = soulcloud::ota_executor::instance().init(&cfg_, &impl_->bridge);
    if (err != ESP_OK) {
        soulcloud::log_sender::instance().deinit();
        esp_timer_delete(impl_->stat_timer);
        impl_->bridge.deinit();
        delete impl_;
        impl_ = nullptr;
        return err;
    }

    inited_ = true;
    ESP_LOGI(TAG, "initialised (uid=%s, broker=%s)", cfg_.device_uid, cfg_.broker_uri);
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::start()
{
    if (!inited_ || impl_ == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (started_) {
        return ESP_OK;
    }
    esp_err_t err = impl_->bridge.start();
    if (err != ESP_OK) {
        return err;
    }
    started_ = true;
    if (impl_->stat_timer != nullptr) {
        esp_timer_start_periodic(impl_->stat_timer, (uint64_t)cfg_.stat_interval_s * 1000000ull);
    }
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::stop()
{
    if (!inited_ || impl_ == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (impl_->stat_timer != nullptr) {
        esp_timer_stop(impl_->stat_timer);
    }
    impl_->bridge.stop();
    started_ = false;
    connected_ = false;
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::deinit()
{
    if (!inited_ || impl_ == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (impl_->stat_timer != nullptr) {
        esp_timer_stop(impl_->stat_timer);
        esp_timer_delete(impl_->stat_timer);
    }
    soulcloud::log_sender::instance().deinit();
    soulcloud::ota_executor::instance().deinit();
    impl_->bridge.deinit();
    delete impl_;
    impl_ = nullptr;
    inited_ = false;
    started_ = false;
    connected_ = false;
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
        .sn = cfg_.serial,
        .sn_len = strlen(cfg_.serial),
        .fw = fw,
        .fw_len = FW_SHA256_LEN,
        .up = up,
        .rst = rst,
    };
    encode_stat(buf, cap, out_len, &stat);
}

esp_err_t soulcloud::soulcloud_client::report_stat()
{
    if (!inited_ || impl_ == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    uint8_t buf[256] = {};
    size_t len = 0;
    build_stat(buf, sizeof(buf), &len);

    char topic[160] = {};
    topic_stat(topic, sizeof(topic), cfg_.device_uid);
    const int32_t msg_id = impl_->bridge.publish(topic, buf, len, 1);
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
    connected_ = true;

    char topic[160] = {};
    topic_cmd_exec(topic, sizeof(topic), cfg_.device_uid);
    impl_->bridge.subscribe(topic, 1);
    topic_ota(topic, sizeof(topic), cfg_.device_uid);
    impl_->bridge.subscribe(topic, 1);

    ESP_LOGI(TAG, "connected; subscribed to cmd/exec and ota");
    report_stat();

    if (conn_cb_ != nullptr) {
        conn_cb_(true, conn_ctx_);
    }
}

void soulcloud::soulcloud_client::on_mqtt_disconnected()
{
    connected_ = false;
    if (conn_cb_ != nullptr) {
        conn_cb_(false, conn_ctx_);
    }
}

void soulcloud::soulcloud_client::on_mqtt_data(const char *topic, size_t topic_len,
                                    const uint8_t *data, size_t data_len)
{
    char expected[160] = {};

    // cmd/exec -> dispatch + result (paused during OTA)
    topic_cmd_exec(expected, sizeof(expected), cfg_.device_uid);
    if (topic_matches(topic, topic_len, expected)) {
        if (soulcloud::ota_executor::instance().is_active()) {
            ESP_LOGD(TAG, "command ignored: OTA in progress");
            return;
        }
        uint8_t result_buf[1024] = {};
        const int32_t n = soulcloud::command_registry::instance().dispatch(data, data_len,
                                                                result_buf, sizeof(result_buf));
        if (n > 0) {
            topic_cmd_result(expected, sizeof(expected), cfg_.device_uid);
            impl_->bridge.publish(expected, result_buf, (size_t)n, 1);
        } else {
            ESP_LOGW(TAG, "cmd/exec dispatch failed (%ld)", (long)n);
        }
        return;
    }

    // ota -> OTA executor
    topic_ota(expected, sizeof(expected), cfg_.device_uid);
    if (topic_matches(topic, topic_len, expected)) {
        ota_notice notice;
        const int32_t rc = decode_ota_notice(data, data_len, &notice);
        if (rc != ERR_OK) {
            ESP_LOGW(TAG, "decode ota notice failed (%ld)", (long)rc);
            return;
        }
        const esp_err_t err = soulcloud::ota_executor::instance().start(&notice);
        if (err != ESP_OK) {
            ESP_LOGW(TAG, "ota start failed: %s", esp_err_to_name(err));
        }
        return;
    }

    ESP_LOGW(TAG, "message on unexpected topic %.*s", (int)topic_len, topic);
}

void soulcloud::soulcloud_client::notify_wifi_connected()
{
    if (impl_ != nullptr) {
        impl_->bridge.notify_wifi_connected();
    }
}
