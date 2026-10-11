#include "sc_log_uplink.hpp"

#include <cstdio>
#include <cstring>

#include <esp_heap_caps.h>
#include <esp_timer.h>
#include <on9log.h>
#include <on9log_fmt.h>

#include "sc_mqtt.hpp"

static_assert(CONFIG_SOULCLOUD_LOG_TEXT_LINE_MAX < CONFIG_SOULCLOUD_LOG_BATCH_SIZE, "a text line must fit in one batch");

sc_log_uplink *sc_log_uplink::active = nullptr;
vprintf_like_t sc_log_uplink::prev_vprintf = nullptr;

#if CONFIG_SOULCLOUD_LOG_BUFFERS_IN_PSRAM
static const constexpr uint32_t BUFFER_CAPS = MALLOC_CAP_SPIRAM;
#else
static const constexpr uint32_t BUFFER_CAPS = MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT;
#endif

esp_err_t sc_log_uplink::init(sc_mqtt *_mqtt, uint32_t _boot_id)
{
    mqtt = _mqtt;
    boot_id = _boot_id;

    sink_lock = xSemaphoreCreateMutex();
    bin_ring = xRingbufferCreateWithCaps(CONFIG_SOULCLOUD_LOG_RINGBUF_SIZE, RINGBUF_TYPE_NOSPLIT, BUFFER_CAPS);
    text_ring = xRingbufferCreateWithCaps(CONFIG_SOULCLOUD_LOG_RINGBUF_SIZE, RINGBUF_TYPE_NOSPLIT, BUFFER_CAPS);
    batch = static_cast<uint8_t *>(heap_caps_malloc(CONFIG_SOULCLOUD_LOG_BATCH_SIZE, BUFFER_CAPS));
    if (sink_lock == nullptr || bin_ring == nullptr || text_ring == nullptr || batch == nullptr) {
        ESP_LOGE(TAG, "init: out of memory");
        return ESP_ERR_NO_MEM;
    }

    if (xTaskCreate(task_handler, "sc_logs", TASK_STACK, this, tskIDLE_PRIORITY + 2, &task) != pdPASS) {
        ESP_LOGE(TAG, "init: can't create task");
        return ESP_ERR_NO_MEM;
    }

    active = this;
#if CONFIG_SOULCLOUD_LOG_TEXT
    prev_vprintf = esp_log_set_vprintf(text_vprintf);
#endif
#if CONFIG_SOULCLOUD_LOG_BIN
    static const on9log_sink_t sink = {
        .start_cb = sink_start,
        .payload_cb = sink_payload,
        .end_cb = sink_end,
    };
    if (on9log_add_sink(&sink, this) != ON9LOG_OK) {
        ESP_LOGE(TAG, "init: can't register the on9log sink");
        return ESP_FAIL;
    }
#endif
    return ESP_OK;
}

void sc_log_uplink::set_text_enabled(bool enabled)
{
    text_enabled.store(enabled);
}

void sc_log_uplink::set_bin_enabled(bool enabled)
{
    bin_enabled.store(enabled);
}

void sc_log_uplink::sink_start(const uint8_t *header, size_t header_len, void *ctx)
{
    auto *uplink = static_cast<sc_log_uplink *>(ctx);
    xSemaphoreTake(uplink->sink_lock, portMAX_DELAY);
    uplink->sink_item = nullptr;
    if (header == nullptr || header_len != sizeof(on9log_packet_header_t) || !uplink->bin_enabled.load()) {
        return; // Plain-text records and a disabled uplink are not stored
    }

    // payload_len in the header is exact, so the whole packet is reserved up front.
    on9log_packet_header_t hdr = {};
    memcpy(&hdr, header, sizeof(hdr));
    size_t total = header_len + hdr.payload_len;
    void *item = nullptr;
    if (xRingbufferSendAcquire(uplink->bin_ring, &item, total, 0) != pdTRUE) {
        uplink->bin_dropped.fetch_add(1);
        return;
    }

    uplink->sink_item = static_cast<uint8_t *>(item);
    uplink->sink_item_len = total;
    memcpy(uplink->sink_item, header, header_len);
    uplink->sink_item_pos = header_len;
}

void sc_log_uplink::sink_payload(const uint8_t *payload, size_t len, size_t, size_t, void *ctx)
{
    auto *uplink = static_cast<sc_log_uplink *>(ctx);
    if (uplink->sink_item == nullptr) {
        return;
    }

    size_t room = uplink->sink_item_len - uplink->sink_item_pos;
    size_t copy_len = len < room ? len : room;
    memcpy(uplink->sink_item + uplink->sink_item_pos, payload, copy_len);
    uplink->sink_item_pos += copy_len;
}

void sc_log_uplink::sink_end(void *ctx)
{
    auto *uplink = static_cast<sc_log_uplink *>(ctx);
    if (uplink->sink_item != nullptr) {
        xRingbufferSendComplete(uplink->bin_ring, uplink->sink_item);
        uplink->sink_item = nullptr;
    }
    xSemaphoreGive(uplink->sink_lock);
}

int sc_log_uplink::text_vprintf(const char *fmt, va_list args)
{
    sc_log_uplink *uplink = active;
    // The upload task's own logs are not uploaded: that would feed itself.
    if (uplink != nullptr && uplink->text_enabled.load() && xTaskGetCurrentTaskHandle() != uplink->task) {
        char line[CONFIG_SOULCLOUD_LOG_TEXT_LINE_MAX];
        va_list copy;
        va_copy(copy, args);
        int len = vsnprintf(line, sizeof(line), fmt, copy);
        va_end(copy);
        if (len > 0) {
            size_t item_len = static_cast<size_t>(len) < sizeof(line) ? len : sizeof(line) - 1;
            if (static_cast<size_t>(len) >= sizeof(line)) {
                line[item_len - 1] = '\n'; // Cut lines still end the line
            }
            if (xRingbufferSend(uplink->text_ring, line, item_len, 0) != pdTRUE) {
                uplink->text_dropped.fetch_add(1);
            }
        }
    }
    return prev_vprintf != nullptr ? prev_vprintf(fmt, args) : vprintf(fmt, args);
}

void sc_log_uplink::task_handler(void *ctx)
{
    auto *uplink = static_cast<sc_log_uplink *>(ctx);
    while (true) {
        vTaskDelay(pdMS_TO_TICKS(CONFIG_SOULCLOUD_LOG_FLUSH_MS));
        if (!uplink->mqtt->connected()) {
            continue; // Logs wait in the ring buffers
        }
        uplink->upload_bin();
        uplink->upload_text();
    }
}

void sc_log_uplink::publish_batch(const char *sub_topic, const char *content_type, size_t len)
{
    sc_publish_props props = {};
    props.content_type = content_type;
#if CONFIG_SOULCLOUD_LOG_QOS1
    const int qos = 1;
#else
    const int qos = 0;
#endif
    auto ret = mqtt->publish(sub_topic, batch, len, qos, &props);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "%s batch of %u bytes lost: 0x%x %s", sub_topic, len, ret, esp_err_to_name(ret));
    }
}

void sc_log_uplink::upload_bin()
{
    const size_t cap = CONFIG_SOULCLOUD_LOG_BATCH_SIZE - TIME_SYNC_LEN; // Room for the closing TIME_SYNC
    size_t len = 0;

    // Report packets dropped since the last batch as an on9log DROPPED packet.
    uint32_t dropped = bin_dropped.exchange(0);
    if (dropped > 0) {
        on9log_packet_header_t hdr = {};
        hdr.magic = ON9LOG_PACKET_MAGIC;
        hdr.type_level = ON9LOG_PKT_DROPPED << 4;
        hdr.time_ms = static_cast<uint32_t>(esp_timer_get_time() / 1000);
        hdr.payload_len = sizeof(dropped);
        memcpy(batch, &hdr, sizeof(hdr));
        memcpy(batch + sizeof(hdr), &dropped, sizeof(dropped));
        len = sizeof(hdr) + sizeof(dropped);
    }

    while (true) {
        size_t item_len = 0;
        auto *item = static_cast<uint8_t *>(xRingbufferReceive(bin_ring, &item_len, 0));
        if (item == nullptr) {
            break;
        }

        if (len + item_len > cap && len > 0) {
            len += on9log_encode_time_sync(batch + len, TIME_SYNC_LEN, boot_id);
            publish_batch("log/bin", "application/octet-stream", len);
            len = 0;
        }

        if (item_len > cap) {
            // A packet bigger than a batch (a large buffer dump) goes out on its own.
            sc_publish_props props = {};
            props.content_type = "application/octet-stream";
            mqtt->publish("log/bin", item, item_len, 0, &props);
        } else {
            memcpy(batch + len, item, item_len);
            len += item_len;
        }
        vRingbufferReturnItem(bin_ring, item);
    }

    if (len > 0) {
        len += on9log_encode_time_sync(batch + len, TIME_SYNC_LEN, boot_id);
        publish_batch("log/bin", "application/octet-stream", len);
    }
}

void sc_log_uplink::upload_text()
{
    const size_t cap = CONFIG_SOULCLOUD_LOG_BATCH_SIZE;
    size_t len = 0;

    uint32_t dropped = text_dropped.exchange(0);
    if (dropped > 0) {
        len = snprintf(
            reinterpret_cast<char *>(batch), cap, "W (%lld) soulcloud: %lu log lines dropped\n", esp_timer_get_time() / 1000,
            static_cast<unsigned long>(dropped)
        );
    }

    while (true) {
        size_t item_len = 0;
        auto *item = static_cast<uint8_t *>(xRingbufferReceive(text_ring, &item_len, 0));
        if (item == nullptr) {
            break;
        }

        if (len + item_len > cap) {
            publish_batch("log/text", "text/plain", len);
            len = 0;
        }
        // Lines are at most CONFIG_SOULCLOUD_LOG_TEXT_LINE_MAX, which is below the batch size.
        memcpy(batch + len, item, item_len);
        len += item_len;
        vRingbufferReturnItem(text_ring, item);
    }

    if (len > 0) {
        publish_batch("log/text", "text/plain", len);
    }
}
