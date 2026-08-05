/*
 * Persistent configuration store: Kconfig defaults with NVS overrides.
 */

#include "soulcloud.hpp"

#include <cstdio>
#include <cstring>

#include <esp_log.h>
#include <esp_mac.h>
#include <nvs.h>

using namespace soulcloud;

static constexpr char TAG[] = "soulcloud_cfg";

/** Reads a string from NVS; on miss/error keeps the default. */
static void load_str(nvs_handle_t h, const char *key, char *out, size_t cap, const char *fallback)
{
    size_t len = cap;
    const esp_err_t err = nvs_get_str(h, key, out, &len);
    if (err == ESP_OK) {
        return;
    }
    if (err != ESP_ERR_NVS_NOT_FOUND) {
        ESP_LOGW(TAG, "nvs_get_str(%s) failed: %s", key, esp_err_to_name(err));
    }
    snprintf(out, cap, "%s", fallback);
}

/** Reads a u32 from NVS; on miss/error keeps the default. */
static uint32_t load_u32(nvs_handle_t h, const char *key, uint32_t fallback)
{
    uint32_t v = 0;
    const esp_err_t err = nvs_get_u32(h, key, &v);
    if (err == ESP_OK) {
        return v;
    }
    if (err != ESP_ERR_NVS_NOT_FOUND) {
        ESP_LOGW(TAG, "nvs_get_u32(%s) failed: %s", key, esp_err_to_name(err));
    }
    return fallback;
}

void soulcloud::config_store::derive_serial(char *out, size_t cap) const
{
    uint8_t mac[6] = {};
    esp_read_mac(mac, ESP_MAC_WIFI_STA);
    snprintf(out, cap, "%02x%02x%02x%02x%02x%02x",
             mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

esp_err_t soulcloud::config_store::load(config *out)
{
    nvs_handle_t h = 0;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READONLY, &h);
    if (err == ESP_ERR_NVS_NOT_FOUND) {
        h = 0;  // no NVS data yet: Kconfig defaults entirely
    } else if (err != ESP_OK) {
        ESP_LOGE(TAG, "nvs_open failed: %s", esp_err_to_name(err));
        return err;
    }

    const config defaults = {
        .device_uid = CONFIG_SOULCLOUD_DEVICE_UID,
        .device_password = CONFIG_SOULCLOUD_DEVICE_PASSWORD,
        .serial = "",
        .broker_uri = CONFIG_SOULCLOUD_BROKER_URI,
        .api_base_url = CONFIG_SOULCLOUD_API_BASE_URL,
        .stat_interval_s = CONFIG_SOULCLOUD_STAT_INTERVAL_S,
        .log_rate_per_s = CONFIG_SOULCLOUD_LOG_RATE_PER_S,
        .log_queue_len = CONFIG_SOULCLOUD_LOG_QUEUE_LEN,
        .mqtt_buffer_in = CONFIG_SOULCLOUD_MQTT_BUFFER_IN,
        .mqtt_buffer_out = CONFIG_SOULCLOUD_MQTT_BUFFER_OUT,
        .mqtt_keepalive_s = CONFIG_SOULCLOUD_MQTT_KEEPALIVE_S,
        .mqtt_reconnect_timeout_ms = CONFIG_SOULCLOUD_MQTT_RECONNECT_TIMEOUT_MS,
        .ota_max_bytes = CONFIG_SOULCLOUD_OTA_MAX_BYTES,
        .ota_timeout_s = CONFIG_SOULCLOUD_OTA_TIMEOUT_S,
    };

    *out = defaults;
    if (h != 0) {
        load_str(h, KEY_UID, out->device_uid, sizeof(out->device_uid), defaults.device_uid);
        load_str(h, KEY_PASS, out->device_password, sizeof(out->device_password), defaults.device_password);
        load_str(h, KEY_SERIAL, out->serial, sizeof(out->serial), defaults.serial);
        load_str(h, KEY_BROKER, out->broker_uri, sizeof(out->broker_uri), defaults.broker_uri);
        load_str(h, KEY_API, out->api_base_url, sizeof(out->api_base_url), defaults.api_base_url);
        out->stat_interval_s = load_u32(h, KEY_STAT_INT, defaults.stat_interval_s);
        out->log_rate_per_s = load_u32(h, KEY_LOG_RATE, defaults.log_rate_per_s);
        out->log_queue_len = load_u32(h, KEY_LOG_Q, defaults.log_queue_len);
        out->mqtt_buffer_in = load_u32(h, KEY_MQTT_IN, defaults.mqtt_buffer_in);
        out->mqtt_buffer_out = load_u32(h, KEY_MQTT_OUT, defaults.mqtt_buffer_out);
        out->mqtt_keepalive_s = load_u32(h, KEY_KA, defaults.mqtt_keepalive_s);
        out->mqtt_reconnect_timeout_ms = load_u32(h, KEY_RECONN, defaults.mqtt_reconnect_timeout_ms);
        out->ota_max_bytes = load_u32(h, KEY_OTA_MAX, defaults.ota_max_bytes);
        out->ota_timeout_s = load_u32(h, KEY_OTA_TO, defaults.ota_timeout_s);
        nvs_close(h);
    }

    if (out->serial[0] == '\0') {
        derive_serial(out->serial, sizeof(out->serial));
    }
    return ESP_OK;
}

esp_err_t soulcloud::config_store::set_string(const char *key, const char *value)
{
    nvs_handle_t h = 0;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err != ESP_OK) {
        return err;
    }
    if (value == nullptr || value[0] == '\0') {
        err = nvs_erase_key(h, key);
        if (err == ESP_ERR_NVS_NOT_FOUND) {
            err = ESP_OK;
        }
    } else {
        err = nvs_set_str(h, key, value);
    }
    if (err == ESP_OK) {
        err = nvs_commit(h);
    }
    nvs_close(h);
    return err;
}

esp_err_t soulcloud::config_store::set_u32(const char *key, uint32_t value)
{
    nvs_handle_t h = 0;
    esp_err_t err = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &h);
    if (err != ESP_OK) {
        return err;
    }
    err = nvs_set_u32(h, key, value);
    if (err == ESP_OK) {
        err = nvs_commit(h);
    }
    nvs_close(h);
    return err;
}
