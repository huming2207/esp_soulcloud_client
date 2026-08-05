/*
 * esp-mqtt bridge implementation.
 */

#include "mqtt_bridge.hpp"

#include <esp_log.h>

namespace soulcloud
{
    esp_err_t mqtt_bridge::init(const config *cfg, const mqtt_callbacks *cbs)
    {
        if (client_ != nullptr) {
            return ESP_ERR_INVALID_STATE;
        }
        if (cbs != nullptr) {
            cbs_ = *cbs;
        }

        memset(&mqtt_cfg_, 0, sizeof(mqtt_cfg_));
        mqtt_cfg_.broker.address.uri = cfg->broker_uri;
        mqtt_cfg_.credentials.client_id = cfg->device_uid;
        mqtt_cfg_.credentials.username = cfg->device_uid;
        mqtt_cfg_.credentials.authentication.password = cfg->device_password;
        mqtt_cfg_.session.keepalive = (int)cfg->mqtt_keepalive_s;
        mqtt_cfg_.session.protocol_ver = MQTT_PROTOCOL_V_3_1_1;
        mqtt_cfg_.buffer.size = (int)cfg->mqtt_buffer_in;
        mqtt_cfg_.buffer.out_size = (int)cfg->mqtt_buffer_out;
        mqtt_cfg_.network.reconnect_timeout_ms = (int)cfg->mqtt_reconnect_timeout_ms;
        mqtt_cfg_.network.disable_auto_reconnect = false;  // auto-reconnect on

        client_ = esp_mqtt_client_init(&mqtt_cfg_);
        if (client_ == nullptr) {
            ESP_LOGE(TAG, "esp_mqtt_client_init failed");
            return ESP_FAIL;
        }

        const esp_err_t err = esp_mqtt_client_register_event(client_, MQTT_EVENT_ANY, event_handler, this);
        if (err != ESP_OK) {
            esp_mqtt_client_destroy(client_);
            client_ = nullptr;
            return err;
        }
        return ESP_OK;
    }

    void mqtt_bridge::deinit()
    {
        if (client_ == nullptr) {
            return;
        }
        if (started_) {
            esp_mqtt_client_stop(client_);
            started_ = false;
        }
        esp_mqtt_client_destroy(client_);
        client_ = nullptr;
        connected_ = false;
    }

    esp_err_t mqtt_bridge::start()
    {
        if (client_ == nullptr || started_) {
            return started_ ? ESP_OK : ESP_ERR_INVALID_STATE;
        }
        const esp_err_t err = esp_mqtt_client_start(client_);
        if (err == ESP_OK) {
            started_ = true;
        }
        return err;
    }

    esp_err_t mqtt_bridge::stop()
    {
        if (client_ == nullptr || !started_) {
            return ESP_OK;
        }
        esp_mqtt_client_disconnect(client_);
        esp_mqtt_client_stop(client_);
        started_ = false;
        connected_ = false;
        return ESP_OK;
    }

    int32_t mqtt_bridge::subscribe(const char *topic, int qos)
    {
        if (client_ == nullptr) {
            return -1;
        }
        return esp_mqtt_client_subscribe(client_, topic, qos);
    }

    int32_t mqtt_bridge::publish(const char *topic, const uint8_t *data, size_t len, int qos)
    {
        if (client_ == nullptr) {
            return -1;
        }
        return esp_mqtt_client_publish(client_, topic, (const char *)data, (int)len, qos, 0);
    }

    void mqtt_bridge::notify_wifi_connected()
    {
        if (client_ == nullptr || !started_ || connected_) {
            return;
        }
        // esp-mqtt reconnects on its own timer; nudge it immediately.
        esp_mqtt_client_reconnect(client_);
    }

    void mqtt_bridge::event_handler(void *ctx, esp_event_base_t base, int32_t evt_id, void *evt_data)
    {
        (void)base;
        (void)evt_id;
        auto *self = static_cast<mqtt_bridge *>(ctx);
        self->handle_event(static_cast<esp_mqtt_event_handle_t>(evt_data));
    }

    void mqtt_bridge::handle_event(esp_mqtt_event_handle_t evt)
    {
        switch (evt->event_id) {
        case MQTT_EVENT_CONNECTED:
            connected_ = true;
            if (cbs_.on_connected != nullptr) {
                cbs_.on_connected(cbs_.ctx);
            }
            break;
        case MQTT_EVENT_DISCONNECTED:
            connected_ = false;
            if (cbs_.on_disconnected != nullptr) {
                cbs_.on_disconnected(cbs_.ctx);
            }
            break;
        case MQTT_EVENT_DATA:
            if (cbs_.on_data != nullptr) {
                cbs_.on_data(cbs_.ctx,
                             evt->topic != nullptr ? evt->topic : "",
                             evt->topic != nullptr ? (size_t)evt->topic_len : 0u,
                             (const uint8_t *)evt->data,
                             (size_t)evt->data_len);
            }
            break;
        case MQTT_EVENT_ERROR:
            if (cbs_.on_error != nullptr) {
                cbs_.on_error(cbs_.ctx, evt->error_handle);
            }
            break;
        default:
            break;
        }
    }
}  // namespace soulcloud
