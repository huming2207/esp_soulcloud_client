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
    // producer -> consumer queue for assembled log packets. Size and
    // memory placement are runtime-configurable (log_rb_size /
    // log_rb_internal; internal SRAM is always safe during OTA flash
    // writes, PSRAM trades that for capacity).
    const uint32_t rb_caps = _cfg->log_rb_internal
                                 ? (MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT)
                                 : (MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    // NOSPLIT: each receive returns exactly one log packet. BYTEBUF
    // would merge back-to-back packets and split items at the wrap point
    // (the consumer treats each item as one complete on9log packet).
    // Packets up to PACKET_MAX (4 KiB) must fit in the buffer.
    log_rb = xRingbufferCreateWithCaps(_cfg->log_rb_size, RINGBUF_TYPE_NOSPLIT, rb_caps);
    if (log_rb == nullptr) {
        _sink_mutex = nullptr;
        return ESP_ERR_NO_MEM;
    }
    // NOSPLIT stores items whole; the largest storable item is roughly
    // half the buffer. Smaller buffers silently drop every packet above
    // that (counted, so visible via the drop WARN, but worth a warning).
    if (_cfg->log_rb_size < 2 * PACKET_MAX) {
        ESP_LOGW(TAG, "log_rb_size %lu < 2*PACKET_MAX (%u): packets near "
                      "%u bytes can never be queued (NOSPLIT half-buffer limit)",
                 (unsigned long)_cfg->log_rb_size, PACKET_MAX, PACKET_MAX);
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
    batch_len = 0;
    batch_elems = 0;
    wake_cb = nullptr;
    wake_ctx = nullptr;
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
        // guard, see drain()). After a successful enqueue, wake the core
        // task so it drains immediately instead of polling.
        if (xRingbufferSend(self->log_rb, self->packet, self->packet_len, 0) != pdTRUE) {
            if (!self->drop_notify_inflight) {
                self->dropped_count++;
            }
        } else if (self->wake_cb != nullptr) {
            self->wake_cb(self->wake_ctx);
        }
    }
    xSemaphoreGive(self->_sink_mutex);
}

void soulcloud::log_sender::drain()
{
    if (log_rb == nullptr || _bridge == nullptr || _cfg == nullptr) {
        return;
    }
    // While disconnected, leave the packets in the ring buffer: on
    // reconnect they are drained and published (batched) instead of
    // dropped, so batching never loses logs to a blip in the uplink.
    if (!_bridge->is_connected()) {
        return;
    }

    const bool batching = _cfg->log_batch_count > 1;

    size_t len = 0;
    uint8_t *item = (uint8_t *)xRingbufferReceive(log_rb, &len, 0);  // never block
    while (item != nullptr) {
        if (len > 0) {
            if (batching) {
                if (!batch_append(item, len)) {
                    flush_batch();  // budget exhausted: send what we have
                    if (!batch_append(item, len)) {
                        // single packet larger than the batch budget:
                        // send it raw rather than dropping it
                        send_packet(item, len);
                    }
                }
                if (batch_elems >= _cfg->log_batch_count ||
                    batch_elems >= BATCH_MAX_ELEMS ||
                    batch_len >= BATCH_MAX_BYTES - 128u) {
                    flush_batch();
                }
            } else {
                send_packet(item, len);  // throttled publish (may drop)
            }
        }
        vRingbufferReturnItem(log_rb, item);
        item = (uint8_t *)xRingbufferReceive(log_rb, &len, 0);
    }

    if (batching && batch_elems > 0) {
        // force-flush triggers: timeout since the batch started, or the
        // ring buffer is close to full (backpressure: flush before
        // packets start getting dropped)
        const uint64_t now = (uint64_t)esp_timer_get_time();
        const bool timeout = _cfg->log_batch_timeout_ms > 0 &&
                             now - batch_start_us >=
                                 (uint64_t)_cfg->log_batch_timeout_ms * 1000ull;
        const bool backpressure =
            xRingbufferGetCurFreeSize(log_rb) < _cfg->log_rb_flush_at;
        if (timeout || backpressure) {
            flush_batch();
        }
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

void soulcloud::log_sender::set_wake(void (*cb)(void *ctx), void *ctx)
{
    wake_cb = cb;
    wake_ctx = ctx;
}

uint64_t soulcloud::log_sender::batch_deadline_us() const
{
    if (_cfg == nullptr || _cfg->log_batch_count <= 1 ||
        _cfg->log_batch_timeout_ms == 0 || batch_elems == 0) {
        return 0;
    }
    return batch_start_us + (uint64_t)_cfg->log_batch_timeout_ms * 1000ull;
}

bool soulcloud::log_sender::throttle_ok()
{
    // throttle to the configured rate (publishes/s): one container or one
    // raw packet consumes one token
    const uint64_t interval_us = 1000000ull / _cfg->log_rate_per_s;
    const uint64_t now = (uint64_t)esp_timer_get_time();
    if (now - last_sent_us < interval_us) {
        return false;
    }
    last_sent_us = now;
    return true;
}

bool soulcloud::log_sender::batch_append(const uint8_t *pkt, size_t len)
{
    if (len == 0 || batch_elems >= BATCH_MAX_ELEMS) {
        return false;
    }
    // element head: bin8 (2 B, len <= 255) or bin16 (3 B)
    const size_t hdr = len <= 255 ? 2u : 3u;
    if (batch_len + hdr + len > BATCH_MAX_BYTES) {
        return false;
    }
    if (batch_elems == 0) {
        batch_start_us = (uint64_t)esp_timer_get_time();
    }
    uint8_t *dst = batch + 4 + batch_len;
    if (len <= 255) {
        dst[0] = 0xc4;
        dst[1] = (uint8_t)len;
    } else {
        dst[0] = 0xc5;
        dst[1] = (uint8_t)(len >> 8);
        dst[2] = (uint8_t)(len & 0xff);
    }
    memcpy(dst + hdr, pkt, len);
    batch_len += hdr + len;
    batch_elems++;
    return true;
}

void soulcloud::log_sender::flush_batch()
{
    if (batch_elems == 0 || _bridge == nullptr || _cfg == nullptr) {
        return;
    }
    // the container consumes one rate-limit token, like a raw packet
    if (!throttle_ok()) {
        return;  // retried on the next drain tick (batch is kept)
    }
    // container head: 0x01 + fixarray (<= 15) or array16
    size_t total = 0;
    if (batch_elems <= 15) {
        memmove(batch + 2, batch + 4, batch_len);  // compact head to 2 B
        batch[0] = 0x01;
        batch[1] = (uint8_t)(0x90 | batch_elems);
        total = 2 + batch_len;
    } else {
        batch[0] = 0x01;
        batch[1] = 0xdc;
        batch[2] = (uint8_t)(batch_elems >> 8);
        batch[3] = (uint8_t)(batch_elems & 0xff);
        total = 4 + batch_len;
    }

    char topic[160] = {};
    topic_log(topic, sizeof(topic), _cfg->device_uid);
    // QoS 0: log uplink is lossy telemetry (drops are counted and
    // surfaced via the drop WARN). QoS 1 with a persistent session made
    // the broker queue unacked log messages during reconnect storms and
    // triggered connection teardowns on slow links (observed in the QEMU
    // CI runner), so logs stay best-effort.
    const int32_t msg_id = _bridge->publish(topic, batch, total, 0);
    if (msg_id < 0) {
        dropped_count++;
    }
    batch_len = 0;
    batch_elems = 0;
}

void soulcloud::log_sender::send_packet(const uint8_t *pkt, size_t len)
{
    if (_bridge == nullptr || _cfg == nullptr || !_bridge->is_connected()) {
        dropped_count++;
        return;
    }
    if (!throttle_ok()) {
        dropped_count++;
        return;
    }

    char topic[160] = {};
    topic_log(topic, sizeof(topic), _cfg->device_uid);
    const int32_t msg_id = _bridge->publish(topic, pkt, len, 0);  // QoS 0, see flush_batch
    if (msg_id < 0) {
        dropped_count++;
    }
    // NOTE: dropped_count is only reset by the drop WARN in drain(); a
    // successful send must not swallow the accumulated count, or the
    // "reconnect and report once" visibility would silently vanish.
}
