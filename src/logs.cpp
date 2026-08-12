/*
 * on9log -> MQTT log sink.
 */

#include "logs.hpp"

#include <cstring>

#include <esp_heap_caps.h>
#include <esp_log.h>
#include <esp_timer.h>
#include <freertos/task.h>
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
    rate_units = (uint64_t)RATE_BURST * RATE_TOKEN_SCALE;
    rate_last_refill_us = (uint64_t)esp_timer_get_time();

    _sink_mutex = xSemaphoreCreateMutexStatic(&_sink_mutex_storage);
    if (_sink_mutex == nullptr) {
        return ESP_ERR_NO_MEM;
    }
    // producer -> consumer queue for assembled log packets. Size and
    // memory placement are runtime-configurable (log_rb_size /
    // log_rb_internal; internal SRAM is always safe during OTA flash
    // writes, PSRAM trades that for capacity).
    const uint32_t rb_caps =
        _cfg->log_rb_internal ? (MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT) : (MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    // NOSPLIT: each receive returns exactly one log packet. BYTEBUF
    // would merge back-to-back packets and split items at the wrap point
    // (the consumer treats each item as one complete on9log packet).
    // Packets up to PACKET_MAX (4 KiB) must fit in the buffer.
    log_rb = xRingbufferCreateWithCaps(_cfg->log_rb_size, RINGBUF_TYPE_NOSPLIT, rb_caps);
    if (log_rb == nullptr) {
        _sink_mutex = nullptr;
        return ESP_ERR_NO_MEM;
    }
    // NOSPLIT also reserves an internal item header, so a simple
    // `buffer >= 2 * item` check is off by that header. Query the created
    // buffer's actual limit and reject configurations that can never queue
    // one legal maximum-size packet.
    const size_t max_item_size = xRingbufferGetMaxItemSize(log_rb);
    if (max_item_size < PACKET_MAX) {
        vRingbufferDelete(log_rb);
        log_rb = nullptr;
        _sink_mutex = nullptr;
        ESP_LOGE(TAG,
                 "log_rb_size %lu too small: NOSPLIT max item is %u "
                 "(PACKET_MAX=%u)",
                 (unsigned long)_cfg->log_rb_size, (unsigned)max_item_size, PACKET_MAX);
        return ESP_ERR_INVALID_ARG;
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
    // Retire the sink before taking its mutex. remove_sink() waits for all
    // readers that already snapshotted this sink. Holding the mutex first
    // can deadlock with such a reader waiting in sink_start().
    on9log_remove_sink(&_sink, this);
    xSemaphoreTake(mtx, portMAX_DELAY);
    if (held_item != nullptr && log_rb != nullptr) {
        vRingbufferReturnItem(log_rb, held_item);
        held_item = nullptr;
        held_item_len = 0;
    }
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
    rate_units = 0;
    rate_last_refill_us = 0;
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

void soulcloud::log_sender::sink_payload(const uint8_t *payload, size_t payload_len, size_t total_arg_cnt, size_t curr_arg_index,
                                         void *ctx)
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
    uint32_t items_since_delay = 0;

    for (;;) {
        // Do not receive an item until single-packet mode has rate credit.
        // vRingbufferReturnItem() releases a received item; it does not put
        // that item back into the queue.
        if (!batching && !rate_credit(1)) {
            break;
        }

        size_t len = held_item_len;
        uint8_t *item = held_item;
        if (item == nullptr) {
            item = (uint8_t *)xRingbufferReceive(log_rb, &len, 0);
        }
        if (item == nullptr) {
            break;
        }

        if (len > 0) {
            if (batching) {
                if (!batch_append(item, len)) {
                    // Keep this already-received item while the current
                    // batch waits for rate credit. This avoids another copy
                    // and, importantly, does not accidentally consume it.
                    if (!flush_batch()) {
                        held_item = item;
                        held_item_len = len;
                        break;
                    }
                    if (!batch_append(item, len)) {
                        // BATCH_MAX_BYTES admits every legal packet. Avoid
                        // pinning a corrupt item forever if that invariant is
                        // ever broken by a future format change.
                        record_drop();
                    }
                }
                if (batch_elems >= _cfg->log_batch_count || batch_elems >= BATCH_MAX_ELEMS ||
                    batch_len >= BATCH_MAX_BYTES - 128u) {
                    (void)flush_batch();
                }
            } else {
                send_packet(item, len);
            }
        }
        held_item = nullptr;
        held_item_len = 0;
        vRingbufferReturnItem(log_rb, item);
        if (++items_since_delay == 16) {
            // Let idle run during a sustained burst so TWDT is fed.
            vTaskDelay(1);
            items_since_delay = 0;
        }
    }

    if (batching && batch_elems > 0) {
        // force-flush triggers: timeout since the batch started, or the
        // ring buffer is close to full (backpressure: flush before
        // packets start getting dropped)
        const uint64_t now = (uint64_t)esp_timer_get_time();
        if (batch_flush_due(now)) {
            (void)flush_batch();
        }
    }

    // Drop visibility: surface accumulated drops as a WARN packet through
    // the normal log path (throttled to one per second, only while the
    // uplink is connected). The notification is emitted with
    // drop_notify_inflight set so its own ring-buffer drop is not counted
    // and it cannot re-trigger itself; while disconnected no notification
    // is emitted at all (the platform already sees the disconnect), and
    // the accumulated count is reported once on reconnect.
    uint32_t dropped = 0;
    if (_bridge->is_connected()) {
        const uint64_t now = (uint64_t)esp_timer_get_time();
        xSemaphoreTake(_sink_mutex, portMAX_DELAY);
        if (dropped_count > 0 && !drop_notify_inflight && now - last_drop_notify_us >= 1000000ull) {
            drop_notify_inflight = true;
            dropped = dropped_count;
            dropped_count = 0;
            last_drop_notify_us = now;
        }
        xSemaphoreGive(_sink_mutex);
        if (dropped > 0) {
            ON9_LOGW("soulcloud", "log uplink dropped %lu packets", (unsigned long)dropped);
            xSemaphoreTake(_sink_mutex, portMAX_DELAY);
            drop_notify_inflight = false;
            xSemaphoreGive(_sink_mutex);
        }
    }
}

void soulcloud::log_sender::set_wake(void (*cb)(void *ctx), void *ctx)
{
    if (_sink_mutex != nullptr) {
        xSemaphoreTake(_sink_mutex, portMAX_DELAY);
    }
    wake_cb = cb;
    wake_ctx = ctx;
    if (_sink_mutex != nullptr) {
        xSemaphoreGive(_sink_mutex);
    }
}

uint64_t soulcloud::log_sender::next_deadline_us() const
{
    if (_cfg == nullptr || _bridge == nullptr || log_rb == nullptr || !_bridge->is_connected()) {
        return 0;
    }

    const uint64_t now = (uint64_t)esp_timer_get_time();
    if (_cfg->log_batch_count <= 1) {
        UBaseType_t waiting = 0;
        vRingbufferGetInfo(log_rb, nullptr, nullptr, nullptr, nullptr, &waiting);
        return waiting > 0 ? rate_deadline_us(1, now) : 0;
    }
    if (batch_elems == 0) {
        return 0;
    }

    uint64_t flush_due = 0;
    if (held_item != nullptr || batch_elems >= _cfg->log_batch_count || batch_elems >= BATCH_MAX_ELEMS ||
        xRingbufferGetCurFreeSize(log_rb) < _cfg->log_rb_flush_at) {
        flush_due = now;
    }
    if (_cfg->log_batch_timeout_ms > 0) {
        const uint64_t timeout_due = batch_start_us + (uint64_t)_cfg->log_batch_timeout_ms * 1000ull;
        if (flush_due == 0 || timeout_due < flush_due) {
            flush_due = timeout_due;
        }
    }
    if (flush_due == 0) {
        return 0;
    }
    // Both the flush trigger and rate credit must be satisfied.
    const uint64_t rate_due = rate_deadline_us(batch_elems, now);
    return flush_due > rate_due ? flush_due : rate_due;
}

void soulcloud::log_sender::refill_rate(uint64_t now)
{
    rate_units = projected_rate_units(now);
    rate_last_refill_us = now;
}

uint64_t soulcloud::log_sender::projected_rate_units(uint64_t now) const
{
    const uint64_t capacity = (uint64_t)RATE_BURST * RATE_TOKEN_SCALE;
    const uint64_t room = capacity - rate_units;
    const uint64_t elapsed = now - rate_last_refill_us;
    const uint64_t rate = _cfg->log_rate_per_s;
    // Compare before multiplying so a very long uptime cannot overflow.
    if (elapsed >= (room + rate - 1u) / rate) {
        return capacity;
    }
    return rate_units + elapsed * rate;
}

bool soulcloud::log_sender::throttle_ok(uint32_t cost)
{
    if (_cfg == nullptr || cost == 0 || cost > RATE_BURST) {
        return false;
    }
    const uint64_t now = (uint64_t)esp_timer_get_time();
    refill_rate(now);
    const uint64_t required = (uint64_t)cost * RATE_TOKEN_SCALE;
    if (rate_units < required) {
        return false;
    }
    rate_units -= required;
    return true;
}

bool soulcloud::log_sender::rate_credit(uint32_t cost) const
{
    if (_cfg == nullptr || cost == 0 || cost > RATE_BURST) {
        return false;
    }
    const uint64_t now = (uint64_t)esp_timer_get_time();
    return rate_deadline_us(cost, now) <= now;
}

uint64_t soulcloud::log_sender::rate_deadline_us(uint32_t cost, uint64_t now) const
{
    const uint64_t available = projected_rate_units(now);
    const uint64_t required = (uint64_t)cost * RATE_TOKEN_SCALE;
    if (available >= required) {
        return now;
    }
    const uint64_t missing = required - available;
    return now + (missing + _cfg->log_rate_per_s - 1u) / _cfg->log_rate_per_s;
}

bool soulcloud::log_sender::batch_flush_due(uint64_t now) const
{
    if (batch_elems == 0) {
        return false;
    }
    const bool timeout = _cfg->log_batch_timeout_ms > 0 && now - batch_start_us >= (uint64_t)_cfg->log_batch_timeout_ms * 1000ull;
    const bool backpressure = held_item != nullptr || xRingbufferGetCurFreeSize(log_rb) < _cfg->log_rb_flush_at;
    return timeout || backpressure;
}

void soulcloud::log_sender::record_drop()
{
    if (_sink_mutex == nullptr) {
        return;
    }
    xSemaphoreTake(_sink_mutex, portMAX_DELAY);
    if (!drop_notify_inflight) {
        dropped_count++;
    }
    xSemaphoreGive(_sink_mutex);
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

bool soulcloud::log_sender::flush_batch()
{
    if (batch_elems == 0 || _bridge == nullptr || _cfg == nullptr) {
        return batch_elems == 0;
    }
    // The backend charges one token per element, so the firmware bucket
    // consumes the same cost for a container.
    if (!throttle_ok(batch_elems)) {
        return false; // retried at next_deadline_us (batch is kept)
    }
    // Always use array16. It is valid for every supported count and avoids
    // moving up to 4 KiB just to compact a two-byte fixarray header.
    batch[0] = 0x01;
    batch[1] = 0xdc;
    batch[2] = (uint8_t)(batch_elems >> 8);
    batch[3] = (uint8_t)(batch_elems & 0xff);
    const size_t total = 4 + batch_len;

    char topic[160] = {};
    topic_log(topic, sizeof(topic), _cfg->device_uid);
    // QoS 0: log uplink is lossy telemetry (drops are counted and
    // surfaced via the drop WARN). QoS 1 with a persistent session made
    // the broker queue unacked log messages during reconnect storms and
    // triggered connection teardowns on slow links (observed in the QEMU
    // CI runner), so logs stay best-effort.
    const int32_t msg_id = _bridge->publish(topic, batch, total, 0);
    if (msg_id < 0) {
        record_drop();
    }
    batch_len = 0;
    batch_elems = 0;
    return true;
}

void soulcloud::log_sender::send_packet(const uint8_t *pkt, size_t len)
{
    if (_bridge == nullptr || _cfg == nullptr || !_bridge->is_connected()) {
        record_drop();
        return;
    }
    if (!throttle_ok(1)) {
        record_drop();
        return;
    }

    char topic[160] = {};
    topic_log(topic, sizeof(topic), _cfg->device_uid);
    const int32_t msg_id = _bridge->publish(topic, pkt, len, 0); // QoS 0, see flush_batch
    if (msg_id < 0) {
        record_drop();
    }
    // NOTE: dropped_count is only reset by the drop WARN in drain(); a
    // successful send must not swallow the accumulated count, or the
    // "reconnect and report once" visibility would silently vanish.
}
