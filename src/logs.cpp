/*
 * on9log -> MQTT log sink.
 */

#include "logs.hpp"

#include <cstring>

#include <esp_heap_caps.h>
#include <esp_log.h>
#include <esp_timer.h>
#include <on9log.h>

#include "mqtt_bridge.hpp"
#include "protocol.hpp"

using namespace soulcloud;

esp_err_t soulcloud::log_sender::init(const config *cfg, mqtt_bridge *bridge)
{
    // The caller's config pointer is borrowed for the lifetime of this
    // singleton (soulcloud_client::init passes &_cfg, its own copy, which
    // outlives the sink — see soulcloud.cpp).
    if (_sink_mutex != nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    _cfg = cfg;
    _bridge = bridge;

    _sink_mutex = xSemaphoreCreateMutexStatic(&_sink_mutex_storage);
    if (_sink_mutex == nullptr) {
        return ESP_ERR_NO_MEM;
    }
    // producer -> consumer queue for assembled log packets
    log_rb = xRingbufferCreateWithCaps(CONFIG_SOULCLOUD_LOG_RB_SIZE, RINGBUF_TYPE_BYTEBUF,
                                       MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (log_rb == nullptr) {
        _sink_mutex = nullptr;
        return ESP_ERR_NO_MEM;
    }

    const on9log_sink_t sink = {
        .start_cb = sink_start,
        .payload_cb = sink_payload,
        .end_cb = sink_end,
    };
    // on9log stores the sink descriptor POINTER in its global table (no
    // copy), so it must outlive the add call — keep it as a member of the
    // singleton instead of a stack temporary (dangling pointer -> garbage
    // callback -> crash on first log packet).
    _sink = sink;
    if (on9log_add_sink(&_sink, this) != ON9LOG_OK) {
        vRingbufferDelete(log_rb);
        log_rb = nullptr;
        _sink_mutex = nullptr;
        return ESP_FAIL;
    }
    ESP_LOGI(TAG, "on9log MQTT sink installed");
    return ESP_OK;
}

void soulcloud::log_sender::deinit()
{
    SemaphoreHandle_t mtx = _sink_mutex;
    if (mtx == nullptr) {
        return;
    }
    // Drain any in-flight sink callbacks before removing the sink: a
    // callback may hold the mutex mid-packet, and remove_sink would wait
    // for its reader slot. Taking the mutex first makes the teardown
    // deterministic (callbacks never wait on deinit).
    xSemaphoreTake(mtx, portMAX_DELAY);
    const on9log_sink_t sink = {
        .start_cb = sink_start,
        .payload_cb = sink_payload,
        .end_cb = sink_end,
    };
    _sink = sink;
    on9log_remove_sink(&_sink, this);
    if (log_rb != nullptr) {
        vRingbufferDelete(log_rb);
        log_rb = nullptr;
    }
    _sink_mutex = nullptr;
    _cfg = nullptr;
    _bridge = nullptr;
    packet_active = false;
    xSemaphoreGive(mtx);
}

void soulcloud::log_sender::sink_start(const uint8_t *header, size_t header_len, void *ctx)
{
    log_sender *self = static_cast<log_sender *>(ctx);
    if (self->_sink_mutex == nullptr) {
        return;
    }
    xSemaphoreTake(self->_sink_mutex, portMAX_DELAY);

    self->packet_len = 0;
    self->packet_active = true;
    self->overflow = false;
    if (header_len > 0) {
        if (header_len <= PACKET_MAX) {
            memcpy(self->packet, header, header_len);
            self->packet_len = header_len;
        } else {
            self->overflow = true;
        }
    }
}

void soulcloud::log_sender::sink_payload(const uint8_t *payload, size_t payload_len,
                              size_t total_arg_cnt, size_t curr_arg_index, void *ctx)
{
    (void)total_arg_cnt;
    (void)curr_arg_index;
    log_sender *self = static_cast<log_sender *>(ctx);
    if (!self->packet_active || self->overflow) {
        return;
    }
    if (payload_len > PACKET_MAX - self->packet_len) {
        self->overflow = true;
        return;
    }
    memcpy(self->packet + self->packet_len, payload, payload_len);
    self->packet_len += payload_len;
}

void soulcloud::log_sender::sink_end(void *ctx)
{
    log_sender *self = static_cast<log_sender *>(ctx);
    // Defensive NULL check: deinit() drains the mutex before clearing
    // _sink_mutex, so a live callback can only hold the old snapshot if
    // teardown races it; skip the packet in that case instead of
    // xSemaphoreGive(NULL).
    if (self->_sink_mutex == nullptr || !self->packet_active) {
        return;
    }
    self->packet_active = false;
    if (!self->overflow && self->log_rb != nullptr) {
        // producer side: zero-tick enqueue; a full ring buffer drops the
        // packet (logs are lossy), the consumer drains in order. A drop
        // of the drop-notification itself is not counted (re-entrancy
        // guard, see drain()).
        if (xRingbufferSend(self->log_rb, self->packet, self->packet_len, 0) != pdTRUE) {
            if (!self->drop_notify_inflight) {
                self->dropped_count++;
            }
        }
    }
    xSemaphoreGive(self->_sink_mutex);
}

void soulcloud::log_sender::drain()
{
    if (log_rb == nullptr || _bridge == nullptr || _cfg == nullptr) {
        return;
    }
    size_t len = 0;
    uint8_t *item = (uint8_t *)xRingbufferReceive(log_rb, &len, 0);  // never block
    while (item != nullptr) {
        if (len > 0) {
            send_packet(item, len);  // throttled publish (may drop)
        }
        vRingbufferReturnItem(log_rb, item);
        item = (uint8_t *)xRingbufferReceive(log_rb, &len, 0);
    }

    // Drop visibility: surface accumulated drops as a WARN packet through
    // the normal log path (throttled to one per second, only while the
    // uplink is connected). The notification is emitted with
    // drop_notify_inflight set so its own ring-buffer drop is not counted
    // and it cannot re-trigger itself; while disconnected no notification
    // is emitted at all (the platform already sees the disconnect), and
    // the accumulated count is reported once on reconnect.
    if (dropped_count > 0 && !drop_notify_inflight && _bridge->is_connected()) {
        const uint64_t now = (uint64_t)esp_timer_get_time();
        if (now - last_drop_notify_us >= 1000000ull) {
            drop_notify_inflight = true;
            ON9_LOGW("soulcloud", "log uplink dropped %lu packets", (unsigned long)dropped_count);
            dropped_count = 0;
            last_drop_notify_us = now;
            drop_notify_inflight = false;
        }
    }
}

void soulcloud::log_sender::send_packet(const uint8_t *pkt, size_t len)
{
    if (_bridge == nullptr || _cfg == nullptr || !_bridge->is_connected()) {
        dropped_count++;
        return;
    }

    // throttle to the configured rate (msg/s)
    const uint64_t interval_us = 1000000ull / _cfg->log_rate_per_s;
    const uint64_t now = (uint64_t)esp_timer_get_time();
    if (now - last_sent_us < interval_us) {
        dropped_count++;
        return;
    }
    last_sent_us = now;

    char topic[160] = {};
    topic_log(topic, sizeof(topic), _cfg->device_uid);
    const int32_t msg_id = _bridge->publish(topic, pkt, len, 0);
    if (msg_id < 0) {
        dropped_count++;
    } else if (dropped_count > 0) {
        ESP_LOGD(TAG, "dropped %lu log packets under throttle", (unsigned long)dropped_count);
        dropped_count = 0;
    }
}
