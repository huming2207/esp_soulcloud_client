#pragma once

#include <atomic>
#include <cstdarg>
#include <cstddef>
#include <cstdint>

#include <esp_err.h>
#include <esp_log.h>
#include <freertos/FreeRTOS.h>
#include <freertos/ringbuf.h>
#include <freertos/semphr.h>
#include <freertos/task.h>

class sc_mqtt;

/**
 * Uploads logs to the platform:
 *  - on9log binary packets (an on9log sink) to d/<id>/up/log/bin, each batch
 *    ending with a TIME_SYNC packet so the platform can place it in time;
 *  - ESP_LOG text lines (an esp_log vprintf hook) to d/<id>/up/log/text.
 *
 * Logging never blocks on the network: each log kind has a no-split ring
 * buffer holding one item per packet or line, and a full buffer drops the
 * new entry and counts it. The upload task drains the buffers into one batch
 * buffer every CONFIG_SOULCLOUD_LOG_FLUSH_MS while connected, and reports
 * dropped entries at the start of the next batch.
 *
 * Memory: two ring buffers and one batch buffer, allocated once by init().
 * A text line is formatted on the logging task's stack
 * (CONFIG_SOULCLOUD_LOG_TEXT_LINE_MAX bytes).
 */
class sc_log_uplink
{
public:
    esp_err_t init(sc_mqtt *_mqtt, uint32_t _boot_id);

    /** Turn the uplinks on or off at runtime (log.config). */
    void set_text_enabled(bool enabled);
    void set_bin_enabled(bool enabled);

private:
    // on9log sink callbacks, called on the logging task
    static void sink_start(const uint8_t *header, size_t header_len, void *ctx);
    static void sink_payload(const uint8_t *payload, size_t len, size_t total_args, size_t arg_index, void *ctx);
    static void sink_end(void *ctx);

    // esp_log hook, called on the logging task
    static int text_vprintf(const char *fmt, va_list args);

    static void task_handler(void *ctx);
    void upload_bin();
    void upload_text();
    void publish_batch(const char *sub_topic, const char *content_type, size_t len);

    sc_mqtt *mqtt = nullptr;
    uint32_t boot_id = 0;
    TaskHandle_t task = nullptr;
    RingbufHandle_t bin_ring = nullptr;
    RingbufHandle_t text_ring = nullptr;
    uint8_t *batch = nullptr; // CONFIG_SOULCLOUD_LOG_BATCH_SIZE
    std::atomic<bool> text_enabled = true;
    std::atomic<bool> bin_enabled = true;
    std::atomic<uint32_t> bin_dropped = 0;
    std::atomic<uint32_t> text_dropped = 0;

    // The packet being written by the sink; sink_lock is held from sink_start() to sink_end().
    SemaphoreHandle_t sink_lock = nullptr;
    uint8_t *sink_item = nullptr;
    size_t sink_item_len = 0;
    size_t sink_item_pos = 0;

    static sc_log_uplink *active; // For the esp_log hook, which has no context argument
    static vprintf_like_t prev_vprintf;

    static const constexpr uint32_t TASK_STACK = 3072;
    static const constexpr size_t TIME_SYNC_LEN = 22;
    static const constexpr char *TAG = "sc_logs";
};
