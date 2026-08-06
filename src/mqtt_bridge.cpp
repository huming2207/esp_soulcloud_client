/*
 * esp-mqtt bridge implementation.
 */

#include "mqtt_bridge.hpp"

#include <esp_log.h>

using namespace soulcloud;

esp_err_t soulcloud::mqtt_bridge::init(const config *cfg, const mqtt_callbacks *cbs)
{
    if (client != nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (cbs != nullptr) {
        _cbs = *cbs;
    }

    memset(&mqtt_cfg, 0, sizeof(mqtt_cfg));
    mqtt_cfg.broker.address.uri = cfg->broker_uri;
    mqtt_cfg.credentials.client_id = cfg->device_uid;
    mqtt_cfg.credentials.username = cfg->device_uid;
    mqtt_cfg.credentials.authentication.password = cfg->device_password;
    mqtt_cfg.session.keepalive = (int)cfg->mqtt_keepalive_s;
    mqtt_cfg.session.protocol_ver = MQTT_PROTOCOL_V_3_1_1;
    mqtt_cfg.buffer.size = (int)cfg->mqtt_buffer_in;
    mqtt_cfg.buffer.out_size = (int)cfg->mqtt_buffer_out;
    mqtt_cfg.network.reconnect_timeout_ms = (int)cfg->mqtt_reconnect_timeout_ms;
    mqtt_cfg.network.disable_auto_reconnect = false;  // auto-reconnect on

    client = esp_mqtt_client_init(&mqtt_cfg);
    if (client == nullptr) {
        ESP_LOGE(TAG, "esp_mqtt_client_init failed");
        return ESP_FAIL;
    }

    const esp_err_t err = esp_mqtt_client_register_event(client, MQTT_EVENT_ANY, event_handler, this);
    if (err != ESP_OK) {
        esp_mqtt_client_destroy(client);
        client = nullptr;
        return err;
    }
    return ESP_OK;
}

void soulcloud::mqtt_bridge::deinit()
{
    if (client == nullptr) {
        return;
    }
    if (started) {
        esp_mqtt_client_stop(client);
        started = false;
    }
    esp_mqtt_client_destroy(client);
    client = nullptr;
    connected = false;
}

esp_err_t soulcloud::mqtt_bridge::start()
{
    if (client == nullptr || started) {
        return started ? ESP_OK : ESP_ERR_INVALID_STATE;
    }
    const esp_err_t err = esp_mqtt_client_start(client);
    if (err == ESP_OK) {
        started = true;
    }
    return err;
}

esp_err_t soulcloud::mqtt_bridge::stop()
{
    if (client == nullptr || !started) {
        return ESP_OK;
    }
    esp_mqtt_client_disconnect(client);
    esp_mqtt_client_stop(client);
    started = false;
    connected = false;
    return ESP_OK;
}

int32_t soulcloud::mqtt_bridge::subscribe(const char *topic, int qos)
{
    if (client == nullptr) {
        return -1;
    }
    return esp_mqtt_client_subscribe(client, topic, qos);
}

int32_t soulcloud::mqtt_bridge::publish(const char *topic, const uint8_t *data, size_t len, int qos)
{
    if (client == nullptr) {
        return -1;
    }
    return esp_mqtt_client_publish(client, topic, (const char *)data, (int)len, qos, 0);
}

void soulcloud::mqtt_bridge::notify_wifi_connected()
{
    if (client == nullptr || !started || connected) {
        return;
    }
    // esp-mqtt reconnects on its own timer; nudge it immediately.
    esp_mqtt_client_reconnect(client);
}

void soulcloud::mqtt_bridge::event_handler(void *ctx, esp_event_base_t base, int32_t evt_id, void *evt_data)
{
    (void)base;
    (void)evt_id;
    auto *self = static_cast<mqtt_bridge *>(ctx);
    self->handle_event(static_cast<esp_mqtt_event_handle_t>(evt_data));
}

void soulcloud::mqtt_bridge::handle_event(esp_mqtt_event_handle_t evt)
{
    switch (evt->event_id) {
    case MQTT_EVENT_CONNECTED:
        connected = true;
        if (_cbs.on_connected != nullptr) {
            _cbs.on_connected(_cbs.ctx);
        }
        break;
    case MQTT_EVENT_DISCONNECTED:
        connected = false;
        if (_cbs.on_disconnected != nullptr) {
            _cbs.on_disconnected(_cbs.ctx);
        }
        break;
    case MQTT_EVENT_DATA:
        if (_cbs.on_data != nullptr) {
            _cbs.on_data(_cbs.ctx,
                         evt->topic != nullptr ? evt->topic : "",
                         evt->topic != nullptr ? (size_t)evt->topic_len : 0u,
                         (const uint8_t *)evt->data,
                         (size_t)evt->data_len);
        }
        break;
    case MQTT_EVENT_ERROR:
        if (_cbs.on_error != nullptr) {
            _cbs.on_error(_cbs.ctx, evt->error_handle);
        }
        break;
    default:
        break;
    }
}
