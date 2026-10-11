#include "sc_mqtt.hpp"

#include <cstdio>
#include <cstring>

#include <esp_crt_bundle.h>
#include <esp_log.h>
#include <mqtt5_client.h>

#include "sc_credentials.hpp"
#include "sc_rpc.hpp"

#if !CONFIG_MQTT_PROTOCOL_5
#error "SoulCloud needs MQTT 5: enable CONFIG_MQTT_PROTOCOL_5"
#endif

esp_err_t sc_mqtt::init(const sc_credentials *creds, sc_rpc *_rpc, size_t out_buffer_size)
{
    rpc = _rpc;
    snprintf(up_topic, sizeof(up_topic), "d/%s/up/", creds->device_id());
    down_topic_len = snprintf(down_topic, sizeof(down_topic), "d/%s/down/", creds->device_id());

    publish_lock = xSemaphoreCreateMutex();
    if (publish_lock == nullptr) {
        return ESP_ERR_NO_MEM;
    }

    esp_mqtt_client_config_t cfg = {};
    cfg.broker.address.uri = creds->mqtt_url();
    cfg.broker.verification.crt_bundle_attach = esp_crt_bundle_attach;
    cfg.credentials.username = creds->device_id();
    cfg.credentials.client_id = creds->device_id();
    cfg.credentials.authentication.password = creds->password();
    cfg.session.protocol_ver = MQTT_PROTOCOL_V_5;
    // Keep the session so requests sent during a short outage are still delivered.
    cfg.session.disable_clean_session = true;
    cfg.session.keepalive = KEEPALIVE_SEC;
    cfg.network.reconnect_timeout_ms = RECONNECT_MS;
    cfg.buffer.out_size = static_cast<int>(out_buffer_size);

    client = esp_mqtt_client_init(&cfg);
    if (client == nullptr) {
        ESP_LOGE(TAG, "init: can't create MQTT client");
        return ESP_ERR_NO_MEM;
    }

    esp_mqtt5_connection_property_config_t conn_props = {};
    conn_props.session_expiry_interval = SESSION_EXPIRY_SEC;
    auto ret = esp_mqtt5_client_set_connect_property(client, &conn_props);
    ret = ret ?: esp_mqtt_client_register_event(client, MQTT_EVENT_ANY, event_handler, this);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "init: can't configure MQTT client: 0x%x %s", ret, esp_err_to_name(ret));
    }
    return ret;
}

esp_err_t sc_mqtt::start()
{
    return esp_mqtt_client_start(client);
}

esp_err_t sc_mqtt::stop()
{
    is_connected.store(false);
    return esp_mqtt_client_stop(client);
}

void sc_mqtt::set_connection_cb(const std::function<void(bool)> &cb)
{
    connection_cb = cb;
}

esp_err_t sc_mqtt::publish(const char *sub_topic, const void *data, size_t len, int qos, const sc_publish_props *props)
{
    char topic[96] = {};
    if (snprintf(topic, sizeof(topic), "%s%s", up_topic, sub_topic) >= static_cast<int>(sizeof(topic))) {
        return ESP_ERR_INVALID_SIZE;
    }
    return publish_topic(topic, data, len, qos, props);
}

esp_err_t sc_mqtt::publish_topic(const char *topic, const void *data, size_t len, int qos, const sc_publish_props *props)
{
    if (strncmp(topic, up_topic, strlen(up_topic)) != 0) {
        return ESP_ERR_INVALID_ARG;
    }
    if (!is_connected.load()) {
        return ESP_ERR_INVALID_STATE;
    }
    if (xSemaphoreTake(publish_lock, pdMS_TO_TICKS(PUBLISH_LOCK_MS)) != pdTRUE) {
        return ESP_ERR_TIMEOUT;
    }

    esp_err_t ret = ESP_OK;
    if (props != nullptr) {
        esp_mqtt5_publish_property_config_t pub_props = {};
        pub_props.content_type = props->content_type;
        pub_props.correlation_data = props->correlation;
        pub_props.correlation_data_len = props->correlation_len;
        pub_props.response_topic = props->response_topic;
        ret = esp_mqtt5_client_set_publish_property(client, &pub_props);
    }

    if (ret == ESP_OK) {
        int msg_id = esp_mqtt_client_publish(client, topic, static_cast<const char *>(data), static_cast<int>(len), qos, 0);
        if (msg_id == -2) {
            ret = ESP_ERR_NO_MEM; // Outbox full
        } else if (msg_id < 0) {
            ret = ESP_FAIL;
        }
    }
    xSemaphoreGive(publish_lock);
    return ret;
}

void sc_mqtt::event_handler(void *ctx, esp_event_base_t base, int32_t event_id, void *event_data)
{
    auto *mqtt = static_cast<sc_mqtt *>(ctx);
    auto *evt = static_cast<esp_mqtt_event_handle_t>(event_data);

    switch (static_cast<esp_mqtt_event_id_t>(event_id)) {
    case MQTT_EVENT_CONNECTED: {
        char filter[40] = {};
        snprintf(filter, sizeof(filter), "%s#", mqtt->down_topic);
        if (esp_mqtt_client_subscribe(evt->client, filter, 1) < 0) {
            ESP_LOGE(TAG, "connected, but can't subscribe to %s; reconnecting", filter);
            esp_mqtt_client_disconnect(evt->client);
            break;
        }
        ESP_LOGI(TAG, "connected");
        mqtt->is_connected.store(true);
        if (mqtt->connection_cb) {
            mqtt->connection_cb(true);
        }
        break;
    }
    case MQTT_EVENT_DISCONNECTED: {
        bool was_connected = mqtt->is_connected.exchange(false);
        if (mqtt->rx_slot != nullptr) {
            mqtt->rpc->give_back(mqtt->rx_slot);
            mqtt->rx_slot = nullptr;
        }
        if (was_connected) {
            ESP_LOGW(TAG, "disconnected");
            if (mqtt->connection_cb) {
                mqtt->connection_cb(false);
            }
        }
        break;
    }
    case MQTT_EVENT_DATA:
        mqtt->handle_data(evt);
        break;
    case MQTT_EVENT_ERROR:
        if (evt->error_handle != nullptr && evt->error_handle->error_type == MQTT_ERROR_TYPE_CONNECTION_REFUSED) {
            ESP_LOGE(TAG, "connection refused (code 0x%x): credentials revoked?", evt->error_handle->connect_return_code);
        }
        break;
    default:
        break;
    }
}

void sc_mqtt::start_request(const esp_mqtt_event_t *evt, const char *method, size_t method_len)
{
    rx_slot = rpc->take_free_slot();
    if (rx_slot == nullptr) {
        ESP_LOGW(TAG, "request %.*s dropped: no free slot", static_cast<int>(method_len), method);
        return;
    }

    // Dotted method names travel as topic levels: down/req/ota/start -> ota.start
    size_t copy_len = method_len < sizeof(rx_slot->method) - 1 ? method_len : sizeof(rx_slot->method) - 1;
    for (size_t idx = 0; idx < copy_len; idx++) {
        rx_slot->method[idx] = method[idx] == '/' ? '.' : method[idx];
    }
    rx_slot->method[copy_len] = '\0';

    const esp_mqtt5_event_property_t *props = evt->property;
    if (props != nullptr && props->correlation_data_len > 0 && props->correlation_data_len <= sizeof(rx_slot->correlation)) {
        memcpy(rx_slot->correlation, props->correlation_data, props->correlation_data_len);
        rx_slot->correlation_len = props->correlation_data_len;
    }
    if (props != nullptr && props->response_topic_len > 0 && props->response_topic_len < static_cast<int>(sizeof(rx_slot->response_topic))) {
        memcpy(rx_slot->response_topic, props->response_topic, props->response_topic_len);
        rx_slot->response_topic[props->response_topic_len] = '\0';
    }
    rx_slot->too_large = evt->total_data_len > CONFIG_SOULCLOUD_RPC_PAYLOAD_MAX;
}

void sc_mqtt::handle_data(const esp_mqtt_event_t *evt)
{
    // The first event of a message carries the topic and properties; later fragments have neither.
    if (evt->current_data_offset == 0) {
        if (rx_slot != nullptr) {
            rpc->give_back(rx_slot);
            rx_slot = nullptr;
        }
        rx_response = false;

        if (evt->topic_len <= static_cast<int>(down_topic_len) || strncmp(evt->topic, down_topic, down_topic_len) != 0) {
            return;
        }
        const char *sub = evt->topic + down_topic_len;
        size_t sub_len = evt->topic_len - down_topic_len;
        if (sub_len > 4 && strncmp(sub, "req/", 4) == 0) {
            start_request(evt, sub + 4, sub_len - 4);
        } else if (sub_len == 3 && strncmp(sub, "res", 3) == 0) {
            rx_response = true;
            rx_correlation_len = 0;
            const esp_mqtt5_event_property_t *props = evt->property;
            if (props != nullptr && props->correlation_data_len <= sizeof(rx_correlation)) {
                memcpy(rx_correlation, props->correlation_data, props->correlation_data_len);
                rx_correlation_len = props->correlation_data_len;
            }
        }
    }

    size_t offset = evt->current_data_offset;
    size_t len = evt->data_len;
    size_t total = evt->total_data_len;
    if (rx_response) {
        rpc->deliver_response(rx_correlation, rx_correlation_len, evt->data, len, offset, total);
        return;
    }
    if (rx_slot == nullptr) {
        return;
    }

    if (!rx_slot->too_large && offset + len <= CONFIG_SOULCLOUD_RPC_PAYLOAD_MAX) {
        memcpy(rx_slot->payload + offset, evt->data, len);
        rx_slot->payload_len = offset + len;
    }
    if (offset + len >= total) {
        rx_slot->payload[rx_slot->payload_len] = '\0';
        rpc->submit(rx_slot);
        rx_slot = nullptr;
    }
}
