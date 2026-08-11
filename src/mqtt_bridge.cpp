/*
 * esp-mqtt bridge implementation.
 */

#include "mqtt_bridge.hpp"

#include <cstring>

#include <esp_heap_caps.h>
#include <esp_log.h>
#include <esp_timer.h>
#ifdef CONFIG_MBEDTLS_CERTIFICATE_BUNDLE
#include <esp_crt_bundle.h>
#endif

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
    // Persistent session: QoS1 messages addressed while the device is
    // briefly offline (commands, OTA notices) are re-delivered after a
    // reconnect instead of being lost, which makes notice delivery
    // robust against short connection drops.
    mqtt_cfg.session.disable_clean_session = true;
    mqtt_cfg.buffer.size = (int)cfg->mqtt_buffer_in;
    mqtt_cfg.buffer.out_size = (int)cfg->mqtt_buffer_out;
    mqtt_cfg.network.reconnect_timeout_ms = (int)cfg->mqtt_reconnect_timeout_ms;
    mqtt_cfg.network.disable_auto_reconnect = false; // auto-reconnect on
#ifdef CONFIG_MBEDTLS_CERTIFICATE_BUNDLE
    // wss:// endpoints verify the server against the built-in CA bundle;
    // without this, secure URIs fail TLS setup in ESP-IDF 6.x (no
    // verification option selected + insecure skip disabled).
    mqtt_cfg.broker.verification.crt_bundle_attach = esp_crt_bundle_attach;
#endif

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

    // reassembly buffer for fragmented inbound PUBLISHes (one complete
    // message at a time; events are delivered strictly serially)
    frag.buf = (uint8_t *)heap_caps_malloc(CONFIG_SOULCLOUD_INBOUND_MAX, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (frag.buf == nullptr) {
        frag.buf = (uint8_t *)heap_caps_malloc(CONFIG_SOULCLOUD_INBOUND_MAX, MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT);
    }
    if (frag.buf == nullptr) {
        esp_mqtt_client_destroy(client);
        client = nullptr;
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}

void soulcloud::mqtt_bridge::deinit()
{
    if (client == nullptr) {
        return;
    }
    if (started.load(std::memory_order_acquire)) {
        esp_mqtt_client_stop(client);
        started.store(false, std::memory_order_release);
    }
    esp_mqtt_client_destroy(client);
    client = nullptr;
    connected.store(false, std::memory_order_release);
    if (frag.buf != nullptr) {
        heap_caps_free(frag.buf);
        frag.buf = nullptr;
    }
    frag.active = false;
}

esp_err_t soulcloud::mqtt_bridge::start()
{
    if (client == nullptr || started.load(std::memory_order_acquire)) {
        return started.load(std::memory_order_relaxed) ? ESP_OK : ESP_ERR_INVALID_STATE;
    }
    const esp_err_t err = esp_mqtt_client_start(client);
    if (err == ESP_OK) {
        started.store(true, std::memory_order_release);
    }
    return err;
}

esp_err_t soulcloud::mqtt_bridge::stop()
{
    if (client == nullptr || !started.load(std::memory_order_acquire)) {
        return ESP_OK;
    }
    esp_mqtt_client_disconnect(client);
    esp_mqtt_client_stop(client);
    started.store(false, std::memory_order_release);
    connected.store(false, std::memory_order_release);
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
    if (client == nullptr || !started.load(std::memory_order_acquire) || connected.load(std::memory_order_acquire)) {
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
        connected.store(true, std::memory_order_release);
        if (_cbs.on_connected != nullptr) {
            _cbs.on_connected(_cbs.ctx);
        }
        break;
    case MQTT_EVENT_DISCONNECTED:
        connected.store(false, std::memory_order_release);
        frag.active = false; // stale fragments are worthless; QoS1 outbox
                             // redelivers the whole message after reconnect
        if (_cbs.on_disconnected != nullptr) {
            _cbs.on_disconnected(_cbs.ctx);
        }
        break;
    case MQTT_EVENT_DATA:
        handle_publish(evt);
        break;
    case MQTT_EVENT_SUBSCRIBED:
        if (_cbs.on_subscribed != nullptr) {
            // data = SUBACK return codes, one per topic; 0x00..0x02 ok,
            // >= 0x80 rejected (only the first topic matters here: we
            // subscribe one topic per call)
            int rc = -1;
            if (evt->data != nullptr && evt->data_len > 0) {
                rc = evt->data[0];
            }
            _cbs.on_subscribed(_cbs.ctx, evt->msg_id, rc, false);
        }
        break;
    case MQTT_EVENT_ERROR:
        // SUBSCRIBE_FAILED is never posted as an ERROR event by esp-mqtt
        // v1.1.0 (SUBACK rejection arrives via MQTT_EVENT_SUBSCRIBED with
        // rc >= 0x80); this branch is defensive only, and evt->msg_id is
        // meaningless for ERROR events in that case.
        if (evt->error_handle != nullptr && evt->error_handle->error_type == MQTT_ERROR_TYPE_SUBSCRIBE_FAILED) {
            if (_cbs.on_subscribed != nullptr) {
                _cbs.on_subscribed(_cbs.ctx, evt->msg_id, -1, true);
            }
        } else if (_cbs.on_error != nullptr) {
            _cbs.on_error(_cbs.ctx, evt->error_handle);
        }
        break;
    default:
        break;
    }
}

/**
 * @brief Handles one MQTT_EVENT_DATA, reassembling fragmented PUBLISHes.
 *
 * A publish larger than the esp-mqtt receive buffer arrives as several
 * events; only the first carries the topic and total_data_len. Fragments
 * are buffered until complete, then forwarded to the core exactly once.
 * Messages whose total length exceeds CONFIG_SOULCLOUD_INBOUND_MAX are
 * dropped whole (the limit now applies to the full payload, not per
 * fragment).
 */
void soulcloud::mqtt_bridge::handle_publish(esp_mqtt_event_handle_t evt)
{
    const uint32_t offset = (uint32_t)evt->current_data_offset;
    const uint32_t total = (uint32_t)evt->total_data_len;
    const size_t data_len = (size_t)evt->data_len;

    if (frag.buf == nullptr) {
        return;
    }

    if (offset == 0 && total == 0) {
        // empty payload: forward as-is (matches pre-reassembly behaviour;
        // the core's strict decoder rejects it with an error result)
        if (_cbs.on_data != nullptr) {
            _cbs.on_data(_cbs.ctx, evt->topic != nullptr ? evt->topic : "", evt->topic != nullptr ? (size_t)evt->topic_len : 0u,
                         (const uint8_t *)evt->data, (size_t)evt->data_len);
        }
        return;
    }

    // The common case is one complete MQTT event. Forward its borrowed
    // buffer directly; the core copies it once into its task-handoff ring
    // buffer. Reassembly storage is only needed for true fragmentation.
    if (offset == 0 && total == data_len && !frag.active) {
        frag.active = false;
        if (_cbs.on_data != nullptr) {
            _cbs.on_data(_cbs.ctx, evt->topic != nullptr ? evt->topic : "", evt->topic != nullptr ? (size_t)evt->topic_len : 0u,
                         (const uint8_t *)evt->data, data_len);
        }
        return;
    }

    if (offset == 0 && total > 0 && !frag.active) {
        // new message: anything mid-stream is protocol garbage, drop it.
        // (!frag.active guard: a zero-length FIRST fragment (possible on
        // slow links) is followed by an offset-0 continuation, which must
        // take the continuation path to preserve the stored topic)
        frag.active = true;
        frag.received = 0;
        frag.total = total;
        if (total > CONFIG_SOULCLOUD_INBOUND_MAX) {
            ESP_LOGW(TAG, "inbound message too large (%u bytes); dropping whole", total);
            frag.active = false;
            return;
        }
        // first fragment carries the topic
        frag.topic_len = 0;
        if (evt->topic != nullptr) {
            const size_t n = evt->topic_len < sizeof(frag.topic) ? evt->topic_len : sizeof(frag.topic) - 1;
            memcpy(frag.topic, evt->topic, n);
            frag.topic[n] = '\0';
            frag.topic_len = n;
        }
    } else if (frag.active && offset == frag.received) {
        // continuation fragment
        if (data_len > frag.total - frag.received) {
            ESP_LOGW(TAG, "fragment overruns message; dropping");
            frag.active = false;
            return;
        }
    } else {
        // unexpected fragment (offset mismatch or no active message)
        frag.active = false;
        ESP_LOGW(TAG, "unexpected fragment offset %u (have %u); dropping", offset, frag.received);
        return;
    }

    if (data_len > 0) {
        if (frag.received + data_len > CONFIG_SOULCLOUD_INBOUND_MAX) {
            frag.active = false;
            return;
        }
        memcpy(frag.buf + frag.received, evt->data, data_len);
    }
    frag.received += data_len;

    if (frag.active && frag.received >= frag.total) {
        // complete message (single-fragment messages take this path too)
        frag.active = false;
        if (_cbs.on_data != nullptr) {
            _cbs.on_data(_cbs.ctx, frag.topic_len > 0 ? frag.topic : "", frag.topic_len, frag.buf, frag.received);
        }
    }
}
