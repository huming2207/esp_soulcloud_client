/*
 * Soulcloud device client core: lifecycle, MQTT event orchestration,
 * periodic stat reports and command dispatch wiring.
 */

#include "soulcloud.hpp"

#include <atomic>
#include <cstdio>
#include <cstring>

#include <esp_app_desc.h>
#include <esp_log.h>
#include <esp_timer.h>
#include <freertos/FreeRTOS.h>
#include <freertos/ringbuf.h>
#include <freertos/task.h>
#include <soc/soc_caps.h>

#include <esp_heap_caps.h>

#include "commands.hpp"
#include "logs.hpp"
#include "mqtt_bridge.hpp"
#include "ota.hpp"
#include "protocol.hpp"

using namespace soulcloud;

const char *soulcloud::soulcloud_client::reset_reason_str(esp_reset_reason_t r)
{
    switch (r) {
    case ESP_RST_UNKNOWN:
        return "UNKNOWN";
    case ESP_RST_POWERON:
        return "POWERON";
    case ESP_RST_EXT:
        return "EXT";
    case ESP_RST_SW:
        return "SW";
    case ESP_RST_PANIC:
        return "PANIC";
    case ESP_RST_INT_WDT:
        return "INT_WDT";
    case ESP_RST_TASK_WDT:
        return "TASK_WDT";
    case ESP_RST_WDT:
        return "WDT";
    case ESP_RST_DEEPSLEEP:
        return "DEEPSLEEP";
    case ESP_RST_BROWNOUT:
        return "BROWNOUT";
    case ESP_RST_SDIO:
        return "SDIO";
    case ESP_RST_USB:
        return "USB";
    case ESP_RST_JTAG:
        return "JTAG";
    case ESP_RST_EFUSE:
        return "EFUSE";
    case ESP_RST_PWR_GLITCH:
        return "PWR_GLITCH";
    case ESP_RST_CPU_LOCKUP:
        return "CPU_LOCKUP";
    default:
        return "UNKNOWN";
    }
}

int soulcloud::soulcloud_client::hex_val(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return 0;
}

void soulcloud::soulcloud_client::hex_to_bin(const char *hex, uint8_t *bin, size_t len)
{
    for (size_t i = 0; i < len; ++i) {
        bin[i] = (uint8_t)((hex_val(hex[i * 2]) << 4) | hex_val(hex[i * 2 + 1]));
    }
}

bool soulcloud::soulcloud_client::topic_matches(const char *topic, size_t topic_len, const char *expected)
{
    const size_t n = strlen(expected);
    return topic_len == n && memcmp(topic, expected, n) == 0;
}

// ------------------------------------------------------------------ //
// PIMPL
// ------------------------------------------------------------------ //

class soulcloud::soulcloud_client::client_impl
{
public:
    explicit client_impl(soulcloud_client &owner) : owner(owner) {}

    mqtt_bridge bridge;
    soulcloud_client &owner;
    // periodic stat deadline (us since boot) maintained by the core task;
    // no esp_timer: publish can block for a network timeout and must not
    // run on the shared esp_timer task
    uint64_t next_stat_us = 0;
    std::atomic<bool> reset_stat_deadline{false};
    std::atomic<bool> connected_work_pending{false};

    // Dedicated inbound dispatch task: command handlers and OTA notice
    // handling are application-ish code with unpredictable stack needs;
    // they must not run on the esp-mqtt (6144 B) or esp_timer (3584 B)
    // task stacks. The MQTT event callback only assembles a small item
    // (header + payload copy) into a FreeRTOS ring buffer and never runs
    // handlers itself; the core task drains the buffer.
    // Created in init and owned/deleted by destroy(). The task parks after
    // publishing task_exited, so producers never observe a stale handle.
    TaskHandle_t task = nullptr;
    RingbufHandle_t inbound_rb = nullptr;
    std::atomic<bool> exit_requested{false};
    std::atomic<bool> task_exited{false};

    // Downlink subscription tracking. Concurrency: the mqtt event task
    // only writes done/retry (on_subscribed); the core task owns
    // msg_id/sent_us and every subscribe() call (single-writer per
    // field, atomics for the cross-task ones). downlink_ready is a
    // one-way latch written by the mqtt task, read by applications.
    struct sub_slot {
        std::atomic<int> msg_id{-1};    // esp-mqtt message id, -1 = none
        bool ota = false;               // true = ota topic, false = cmd/exec
        std::atomic<bool> done{false};  // SUBACK received and accepted
        std::atomic<bool> retry{false}; // rejected/failed: re-issue needed
        std::atomic<uint64_t> sent_us{0};
    };
    sub_slot subs[2] = {};
    std::atomic<bool> downlink_ready{false};
    std::atomic<uint32_t> sub_gen{0}; // bumped on connect/disconnect

    enum inbound_kind : uint8_t {
        INBOUND_NONE = 0,
        INBOUND_CMD, // cmd/exec payload
        INBOUND_OTA, // ota notice payload
    };

    /** Ring buffer item header (4 bytes, aligned). */
    struct inbound_header {
        uint8_t kind; // inbound_kind
        uint8_t pad;  // keep len 16-bit aligned
        uint16_t len; // payload length
    };

    // C-callback trampolines
    static void mqtt_on_connected(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner.on_mqtt_connected();
    }

    static void mqtt_on_disconnected(void *ctx)
    {
        static_cast<client_impl *>(ctx)->owner.on_mqtt_disconnected();
    }

    static void mqtt_on_subscribed(void *ctx, int msg_id, int return_code, bool failed)
    {
        static_cast<client_impl *>(ctx)->owner.on_subscribed(msg_id, return_code, failed);
    }

    static void mqtt_on_data(void *ctx, const char *topic, size_t topic_len, const uint8_t *data, size_t data_len)
    {
        static_cast<client_impl *>(ctx)->owner.on_mqtt_data(topic, topic_len, data, data_len);
    }

    static void task_main(void *ctx)
    {
        static_cast<client_impl *>(ctx)->run();
    }

    /**
     * Wakes the core task via a task notification.
     *
     * Task-context only (the SMP kernel takes a kernel spinlock inside
     * xTaskGenericNotify; an ISR would need the FromISR variant). Must
     * The handle remains live until destroy() has stopped all producers.
     * Notifications coalesce, which is fine because every wake drains the
     * queues.
     */
    void wake_core()
    {
        if (task != nullptr) {
            xTaskNotifyGive(task);
        }
    }

    /** Trampoline for log_sender::set_wake (called from the log source's
     *  task after a successful enqueue). */
    static void wake_cb_trampoline(void *ctx)
    {
        static_cast<client_impl *>(ctx)->wake_core();
    }

    /**
     * Computes how long run() may block in xTaskNotifyWait before it must
     * wake up on its own: the earliest of
     *   - the periodic stat deadline (next_stat_us),
     *   - the SUBACK watchdog deadline (10 s after the last subscribe
     *     issue, only for subscriptions that are actually in flight), and
     *   - the log batch force-flush deadline (batch_start + timeout).
     * Returns 0 when work is already due (do not block), or portMAX_DELAY
     * when nothing is scheduled (block until the next notification).
     */
    uint32_t deadline_wait_ticks() const
    {
        uint32_t wait_ticks = portMAX_DELAY;
        const uint64_t now = (uint64_t)esp_timer_get_time();

        // periodic stat: report on the core task (never on esp_timer)
        if (owner.started.load(std::memory_order_acquire)) {
            if (now >= next_stat_us) {
                return 0;
            }
            const uint32_t t = (uint32_t)pdMS_TO_TICKS((next_stat_us - now) / 1000ull) + 1;
            if (t < wait_ticks) {
                wait_ticks = t;
            }
        }

        // SUBACK watchdog: only subscriptions with a live message id are
        // time-bound; ids < 0 (not connected, or subscribe refused) are
        // re-issued on the next connect/notification instead
        for (const auto &sub : subs) {
            if (sub.done.load(std::memory_order_relaxed)) {
                continue;
            }
            const int mid = sub.msg_id.load(std::memory_order_relaxed);
            const uint64_t sent = sub.sent_us.load(std::memory_order_relaxed);
            if (mid >= 0 && sent > 0) {
                const uint64_t due = sent + 10 * 1000000ull;
                if (now >= due) {
                    return 0;
                }
                const uint32_t t = (uint32_t)pdMS_TO_TICKS((due - now) / 1000ull) + 1;
                if (t < wait_ticks) {
                    wait_ticks = t;
                }
            }
        }

        // log batch force-flush deadline (batching mode only). While
        // disconnected the batch is intentionally kept (drain() returns
        // early), so no deadline is scheduled at all: the reconnect wake
        // handles the flush. While connected, a due batch returns 1 tick
        // instead of 0: flush_batch may be held by the rate limiter, and
        // spinning at 0 would busy-loop the core task for the whole
        // throttle window.
        const uint64_t batch_due = soulcloud::log_sender::instance().next_deadline_us();
        if (batch_due != 0 && bridge.is_connected()) {
            if (now >= batch_due) {
                return 1;
            }
            const uint32_t t = (uint32_t)pdMS_TO_TICKS((batch_due - now) / 1000ull) + 1;
            if (t < wait_ticks) {
                wait_ticks = t;
            }
        }

        return wait_ticks;
    }

    void process_connected_work()
    {
        if (!bridge.is_connected() || !connected_work_pending.exchange(false, std::memory_order_acq_rel)) {
            return;
        }

        // Keep blocking publish/NVS work off the esp-mqtt event task.
        owner.report_stat();
        soulcloud::ota_executor::instance().finalize_pending_ota();
        next_stat_us = (uint64_t)esp_timer_get_time() + (uint64_t)owner._cfg.stat_interval_s * 1000000ull;
    }

    void run()
    {
        for (;;) {
            // Event-driven wait (ESP-07): block until a producer notifies
            // us (log sink enqueue, inbound MQTT message, connect event,
            // teardown) or the earliest scheduled deadline elapses. No
            // polling: idle wakeups drop from 100/s to ~0, and latency is
            // bounded by the producer's notify instead of a 10 ms tick.
            const uint32_t wait_ticks = deadline_wait_ticks();
            uint32_t notified = 0;
            xTaskNotifyWait(0, ULONG_MAX, &notified, wait_ticks);
            (void)notified; // spurious wakes are harmless: drains are cheap
            if (exit_requested.load(std::memory_order_acquire)) {
                break;
            }
            if (reset_stat_deadline.exchange(false, std::memory_order_acq_rel)) {
                next_stat_us = 0;
            }

            // drain inbound (commands/OTA) without blocking
            size_t len = 0;
            uint8_t *item = (uint8_t *)xRingbufferReceive(inbound_rb, &len, 0);
            uint32_t inbound_since_delay = 0;
            while (item != nullptr) {
                if (len >= sizeof(inbound_header)) {
                    // NOSPLIT items are 32-bit aligned and inbound_header is
                    // four bytes, so read the ring-buffer header in place.
                    const inbound_header *hdr = reinterpret_cast<const inbound_header *>(item);
                    const uint8_t *payload = item + sizeof(*hdr);
                    const size_t plen = hdr->len;
                    if (plen <= len - sizeof(*hdr)) { // truncated-item guard
                        switch (hdr->kind) {
                        case INBOUND_CMD:
                            owner.dispatch_command(payload, plen);
                            break;
                        case INBOUND_OTA:
                            owner.handle_ota_notice(payload, plen);
                            break;
                        default:
                            ESP_LOGW(TAG, "unknown inbound kind %u", (unsigned)hdr->kind);
                            break;
                        }
                    }
                }
                vRingbufferReturnItem(inbound_rb, item);
                if (++inbound_since_delay == 8) {
                    // A sustained command burst must still let the idle task
                    // run and feed the task watchdog.
                    vTaskDelay(1);
                    inbound_since_delay = 0;
                }
                item = (uint8_t *)xRingbufferReceive(inbound_rb, &len, 0);
            }

            // SUBACK watchdog: the only writer of msg_id/sent_us and the
            // only caller of subscribe() (single-writer discipline).
            // Re-issues when: no SUBACK within 10 s, subscribe() returned
            // -1 (esp-mqtt refuses when not connected), or the broker
            // rejected the subscription (retry flag from on_subscribed).
            // A generation counter guards against stale write-backs
            // racing a reconnect. Backoff prevents -1 slots from being
            // re-issued every tick while disconnected.
            {
                const uint64_t now = (uint64_t)esp_timer_get_time();
                if (bridge.is_connected()) {
                    for (auto &sub : subs) {
                        const int mid = sub.msg_id.load(std::memory_order_relaxed);
                        const bool want = !sub.done.load(std::memory_order_relaxed) &&
                                          (sub.retry.load(std::memory_order_relaxed) || mid < 0 ||
                                           now - sub.sent_us.load(std::memory_order_relaxed) > 10 * 1000000ull);
                        if (!want) {
                            continue;
                        }
                        char topic[160] = {};
                        if (sub.ota) {
                            topic_ota(topic, sizeof(topic), owner._cfg.device_uid);
                        } else {
                            topic_cmd_exec(topic, sizeof(topic), owner._cfg.device_uid);
                        }
                        const uint32_t gen = sub_gen.load(std::memory_order_relaxed);
                        const int new_id = (int)bridge.subscribe(topic, 1);
                        if (gen == sub_gen.load(std::memory_order_relaxed)) {
                            sub.retry.store(false, std::memory_order_relaxed);
                            sub.msg_id.store(new_id, std::memory_order_relaxed);
                            sub.sent_us.store(now, std::memory_order_relaxed);
                        }
                    }
                }
            }

            process_connected_work();

            // Control-plane work above gets priority over lossy log
            // telemetry, especially after reconnect with a full log queue.
            soulcloud::log_sender::instance().drain();

            // periodic stat report (publish may block for a network
            // timeout; that is acceptable here on the core task, but must
            // never happen on the shared esp_timer task)
            const uint64_t now = (uint64_t)esp_timer_get_time();
            if (owner.started.load(std::memory_order_acquire) && now >= next_stat_us) {
                if (owner.impl != nullptr && owner.impl->bridge.is_connected()) {
                    owner.report_stat();
                }
                next_stat_us = now + (uint64_t)owner._cfg.stat_interval_s * 1000000ull;
            }

            if (exit_requested.load(std::memory_order_acquire)) {
                break;
            }
            // loop back into xTaskNotifyWait (no fixed delay)
        }
        task_exited.store(true, std::memory_order_release);
        // Keep the task/handle alive until destroy() stops MQTT and log
        // producers, then deletes this parked worker. A notification is
        // deliberately unable to release this park: MQTT may still notify
        // between task_exited and bridge.deinit(), and returning from the
        // task entry point would leave destroy() with a stale handle.
        vTaskSuspend(nullptr);
    }

    /**
     * @brief Shared teardown: every init() error path and deinit() funnel
     *        through here, so a partial init can never leak the core task
     *        or ring buffers nor leave them dangling.
     *
     * Order matters (use-after-free audit):
     *  1. The core task consumes inbound_rb and the log ring buffer, so it
     *     must park first. It remains allocated until every producer has
     *     stopped; forced deletion is a watchdog-only fallback.
     *  2. The MQTT client is destroyed BEFORE the inbound ring buffer:
     *     the esp-mqtt event task may be blocked inside on_mqtt_data() on
     *     xRingbufferSend() (100 ms backpressure wait), and
     *     esp_mqtt_client_destroy() waits for that task to exit. Deleting
     *     the ring buffer first used to be a use-after-free (M1).
     *  3. Only then are the ring buffers deleted and the singleton
     *     sub-components (log sink, OTA executor) torn down; both are
     *     idempotent when their init never ran.
     */
    static void destroy(soulcloud_client &self)
    {
        client_impl *impl = self.impl;
        if (impl == nullptr) {
            return;
        }

        bool core_forced = false;
        if (impl->task != nullptr) {
            // Stop log callbacks from adding notifications before the core
            // publishes its parked state. MQTT callbacks may still notify,
            // but the handle remains live until bridge.deinit() below.
            soulcloud::log_sender::instance().set_wake(nullptr, nullptr);
            impl->exit_requested.store(true, std::memory_order_release);
            impl->wake_core(); // the task blocks in xTaskNotifyWait; nudge it
            // Wait generously (30 s): the core task may be inside
            // esp_mqtt_client_publish(), which blocks for up to the
            // network timeout, and deleting it there would corrupt the
            // esp-mqtt client state for the subsequent bridge.deinit()
            // (ESP-R5). Command handlers are application code and may
            // legitimately take long too. The forced delete below is a
            // last resort for a wedged task.
            const TickType_t wait_ticks = pdMS_TO_TICKS(30000);
            for (TickType_t i = 0; i < wait_ticks && !impl->task_exited.load(std::memory_order_acquire); ++i) {
                vTaskDelay(1);
            }
            if (!impl->task_exited.load(std::memory_order_acquire)) {
                core_forced = true;
                ESP_LOGE(TAG, "core task did not stop within 30 s; force-deleting");
                vTaskDelete(impl->task);
                impl->task = nullptr;
            }
        }

        // Stop the OTA executor BEFORE the MQTT client: its deinit waits
        // (bounded) for an in-flight OTA task, which may still be
        // publishing ota/result reports through the bridge. Destroying
        // the bridge first would lose those reports and, in an extreme
        // interleaving, let the OTA task touch a freed client.
        soulcloud::ota_executor::instance().deinit();

        // stop the MQTT client and wait for its event task to exit before
        // deleting the ring buffer it can be blocked on
        impl->bridge.deinit();

        if (impl->inbound_rb != nullptr) {
            vRingbufferDelete(impl->inbound_rb);
            impl->inbound_rb = nullptr;
        }
        soulcloud::log_sender::instance().deinit();
        if (impl->task != nullptr && !core_forced) {
            vTaskDelete(impl->task);
        }
        impl->task = nullptr;
        delete impl;
        self.impl = nullptr;
    }
};

// ------------------------------------------------------------------ //
// lifecycle
// ------------------------------------------------------------------ //

esp_err_t soulcloud::soulcloud_client::init(const config *cfg)
{
    if (inited) {
        return ESP_ERR_INVALID_STATE;
    }
    if (cfg == nullptr || cfg->device_uid[0] == '\0' || cfg->broker_uri[0] == '\0') {
        return ESP_ERR_INVALID_ARG;
    }
    // Scalar sanity: config_store::load() clamps NVS values, but init()
    // also accepts hand-built configs; a zero here would divide by zero
    // in the log sink (log_rate_per_s) or make the stat timer fire
    // continuously (stat_interval_s).
    if (cfg->log_rate_per_s == 0 || cfg->stat_interval_s == 0) {
        return ESP_ERR_INVALID_ARG;
    }

    _cfg = *cfg;
    impl = new client_impl(*this); // sole heap allocation (deinit frees)

    const mqtt_callbacks cbs = {
        .on_connected = client_impl::mqtt_on_connected,
        .on_disconnected = client_impl::mqtt_on_disconnected,
        .on_data = client_impl::mqtt_on_data,
        .on_subscribed = client_impl::mqtt_on_subscribed,
        .on_error = nullptr,
        .ctx = impl,
    };

    esp_err_t err = impl->bridge.init(cfg, &cbs);
    if (err != ESP_OK) {
        client_impl::destroy(*this);
        return err;
    }

    // Dedicated inbound dispatch task: handlers are application code with
    // unpredictable stack needs, so they run here (large, PSRAM-backed
    // stack) instead of on the esp-mqtt/esp_timer task stacks. Inbound
    // messages queue up in a FreeRTOS NOSPLIT ring buffer, so a burst
    // of commands/notices is drained in order instead of dropped.
    // NOSPLIT: every xRingbufferReceive returns exactly one item (one
    // [header][payload] record). BYTEBUF is a byte stream that merges
    // back-to-back sends and splits items at the wrap point, which would
    // corrupt the message framing the consumer relies on. The maximum
    // record (4 + CONFIG_SOULCLOUD_INBOUND_MAX) must fit in the buffer.
    impl->inbound_rb =
        xRingbufferCreateWithCaps(CONFIG_SOULCLOUD_INBOUND_RB_SIZE, RINGBUF_TYPE_NOSPLIT, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (impl->inbound_rb == nullptr) {
        client_impl::destroy(*this);
        return ESP_ERR_NO_MEM;
    }
    // NOSPLIT stores items whole; the largest storable item is roughly
    // half the buffer. Refuse to run with a configuration where a legal
    // maximum-size record can never be queued (permanent silent drops).
    if (CONFIG_SOULCLOUD_INBOUND_RB_SIZE < 2 * (sizeof(client_impl::inbound_header) + CONFIG_SOULCLOUD_INBOUND_MAX)) {
        ESP_LOGE(TAG,
                 "SOULCLOUD_INBOUND_RB_SIZE %d too small: must be >= %u "
                 "(NOSPLIT half-buffer limit)",
                 CONFIG_SOULCLOUD_INBOUND_RB_SIZE,
                 2u * (unsigned)(sizeof(client_impl::inbound_header) + CONFIG_SOULCLOUD_INBOUND_MAX));
        client_impl::destroy(*this);
        return ESP_ERR_INVALID_ARG;
    }
    TaskHandle_t task = nullptr;
    uint32_t task_caps = MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT;
#if CONFIG_SOULCLOUD_CORE_TASK_STACK_PSRAM
    task_caps = MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT;
#endif
    err = xTaskCreateWithCaps(client_impl::task_main, "soulcloud_core", CONFIG_SOULCLOUD_CORE_TASK_STACK, impl, 5, &task,
                              task_caps);
    impl->task = task;
    if (err != pdPASS) {
        client_impl::destroy(*this);
        return ESP_ERR_NO_MEM;
    }

    // NOTE: pass the copy (cfg) not the caller's stack pointer: log_sender
    // and ota_executor keep the pointer for their whole lifetime, and the
    // caller's `cfg` lives on the main_task stack only until init() returns.
    err = soulcloud::log_sender::instance().init(&_cfg, &impl->bridge);
    if (err != ESP_OK) {
        client_impl::destroy(*this);
        return err;
    }
    // wake the core task whenever a log packet is enqueued, so the
    // event-driven consumer drains immediately instead of polling
    soulcloud::log_sender::instance().set_wake(client_impl::wake_cb_trampoline, impl);

    err = soulcloud::ota_executor::instance().init(&_cfg, &impl->bridge);
    if (err != ESP_OK) {
        client_impl::destroy(*this);
        return err;
    }

    inited = true;
    ESP_LOGI(TAG, "initialised (uid=%s, broker=%s)", _cfg.device_uid, _cfg.broker_uri);
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::start()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    if (started.load(std::memory_order_acquire)) {
        return ESP_OK;
    }
    esp_err_t err = impl->bridge.start();
    if (err != ESP_OK) {
        return err;
    }
    started.store(true, std::memory_order_release);
    impl->reset_stat_deadline.store(true, std::memory_order_release);
    impl->wake_core();
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::stop()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    impl->bridge.stop();
    started.store(false, std::memory_order_release);
    connected.store(false, std::memory_order_release);
    impl->wake_core();
    return ESP_OK;
}

esp_err_t soulcloud::soulcloud_client::deinit()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    client_impl::destroy(*this);
    inited = false;
    started.store(false, std::memory_order_release);
    connected.store(false, std::memory_order_release);
    return ESP_OK;
}

// ------------------------------------------------------------------ //
// commands
// ------------------------------------------------------------------ //

esp_err_t soulcloud::soulcloud_client::register_command(const char *name, command_handler_t handler, void *ctx)
{
    return soulcloud::command_registry::instance().register_command(name, handler, ctx);
}

// ------------------------------------------------------------------ //
// stat
// ------------------------------------------------------------------ //

void soulcloud::soulcloud_client::build_stat(uint8_t *buf, size_t cap, size_t *out_len)
{
    char fw_hex[65] = {};
    esp_app_get_elf_sha256(fw_hex, sizeof(fw_hex));
    uint8_t fw[FW_SHA256_LEN] = {};
    hex_to_bin(fw_hex, fw, FW_SHA256_LEN);

    const char *rst = reset_reason_str(esp_reset_reason());
    const uint64_t up = (uint64_t)(esp_timer_get_time() / 1000000);

    const device_stat stat = {
        .sn = _cfg.serial,
        .sn_len = strlen(_cfg.serial),
        .fw = fw,
        .fw_len = FW_SHA256_LEN,
        .up = up,
        .rst = rst,
    };
    encode_stat(buf, cap, out_len, &stat);
}

esp_err_t soulcloud::soulcloud_client::report_stat()
{
    if (!inited || impl == nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    uint8_t buf[256] = {};
    size_t len = 0;
    build_stat(buf, sizeof(buf), &len);

    char topic[160] = {};
    topic_stat(topic, sizeof(topic), _cfg.device_uid);
    const int32_t msg_id = impl->bridge.publish(topic, buf, len, 1);
    if (msg_id < 0) {
        ESP_LOGW(TAG, "stat publish failed");
        return ESP_FAIL;
    }
    ESP_LOGD(TAG, "stat published (%u bytes)", (unsigned)len);
    return ESP_OK;
}

// ------------------------------------------------------------------ //
// MQTT event handling
// ------------------------------------------------------------------ //

void soulcloud::soulcloud_client::on_mqtt_connected()
{
    connected.store(true, std::memory_order_release);

    // Reset the subscription tracking; the actual subscribe() calls are
    // issued by the core-task watchdog (single writer for msg_id/sent_us,
    // see sub_slot). downlink_ready latches once both SUBACKs arrive.
    impl->sub_gen.fetch_add(1, std::memory_order_relaxed);
    impl->downlink_ready.store(false, std::memory_order_relaxed);
    impl->subs[0].ota = false;
    impl->subs[1].ota = true;
    for (auto &sub : impl->subs) {
        sub.done.store(false, std::memory_order_relaxed);
        sub.retry.store(false, std::memory_order_relaxed);
        sub.msg_id.store(-1, std::memory_order_relaxed);
        sub.sent_us.store(0, std::memory_order_relaxed);
    }

    ESP_LOGI(TAG, "connected; downlink subscriptions pending");

    // The core task owns blocking stat/NVS work and prioritises it plus
    // subscription recovery ahead of buffered logs.
    impl->connected_work_pending.store(true, std::memory_order_release);
    impl->wake_core();

    if (conn_cb != nullptr) {
        conn_cb(true, conn_ctx);
    }
}

void soulcloud::soulcloud_client::on_mqtt_disconnected()
{
    connected.store(false, std::memory_order_release);
    impl->connected_work_pending.store(false, std::memory_order_release);
    impl->sub_gen.fetch_add(1, std::memory_order_relaxed);
    impl->downlink_ready.store(false, std::memory_order_relaxed);
    for (auto &sub : impl->subs) {
        sub.done.store(false, std::memory_order_relaxed);
        sub.retry.store(false, std::memory_order_relaxed);
        sub.msg_id.store(-1, std::memory_order_relaxed);
        sub.sent_us.store(0, std::memory_order_relaxed);
    }
    if (conn_cb != nullptr) {
        conn_cb(false, conn_ctx);
    }
}

bool soulcloud::soulcloud_client::downlink_ready() const
{
    return impl != nullptr && impl->downlink_ready;
}

void soulcloud::soulcloud_client::on_subscribed(int msg_id, int return_code, bool failed)
{
    if (impl == nullptr) {
        return;
    }
    // single-writer discipline: this runs on the mqtt event task and only
    // flips done/retry; the core-task watchdog owns subscribe() calls
    for (auto &sub : impl->subs) {
        if (sub.msg_id.load(std::memory_order_relaxed) != msg_id || sub.done.load(std::memory_order_relaxed)) {
            continue;
        }
        if (failed || return_code >= 0x80) {
            ESP_LOGW(TAG, "subscribe rejected (rc=%d); will retry", return_code);
            sub.retry.store(true, std::memory_order_relaxed);
            // wake the core task so the retry happens now instead of
            // waiting for the 10 s watchdog deadline
            impl->wake_core();
        } else {
            sub.done.store(true, std::memory_order_relaxed);
        }
        break;
    }
    if (impl->subs[0].done.load(std::memory_order_relaxed) && impl->subs[1].done.load(std::memory_order_relaxed)) {
        impl->downlink_ready.store(true, std::memory_order_relaxed);
        // explicit readiness line for the harness and operators; the E2E
        // tests wait for this instead of the (removed) "subscribed" log
        ESP_LOGI(TAG, "downlink ready (cmd/exec + ota subscribed)");
    }
}

void soulcloud::soulcloud_client::on_mqtt_data(const char *topic, size_t topic_len, const uint8_t *data, size_t data_len)
{
    if (impl == nullptr || impl->inbound_rb == nullptr) {
        return;
    }

    // Classify the topic (cheap) and enqueue header + payload copy into
    // the core task's ring buffer. Handlers run on the dedicated core
    // task: they are application code and must not execute on the small
    // esp-mqtt event task stack.
    char expected[160] = {};

    client_impl::inbound_kind kind = client_impl::INBOUND_NONE;
    topic_cmd_exec(expected, sizeof(expected), _cfg.device_uid);
    if (topic_matches(topic, topic_len, expected)) {
        kind = client_impl::INBOUND_CMD;
    } else {
        topic_ota(expected, sizeof(expected), _cfg.device_uid);
        if (topic_matches(topic, topic_len, expected)) {
            kind = client_impl::INBOUND_OTA;
        }
    }
    if (kind == client_impl::INBOUND_NONE) {
        ESP_LOGW(TAG, "message on unexpected topic %.*s", (int)topic_len, topic);
        return;
    }

    if (data_len > CONFIG_SOULCLOUD_INBOUND_MAX) {
        ESP_LOGW(TAG, "inbound payload too large (%u bytes); dropping", (unsigned)data_len);
        return;
    }

    // Reserve the final NOSPLIT item and write into it directly. This
    // removes the per-message heap allocation and the staging copy.
    const size_t item_len = sizeof(client_impl::inbound_header) + data_len;
    client_impl::inbound_header *item = nullptr;
    if (xRingbufferSendAcquire(impl->inbound_rb, (void **)&item, item_len, pdMS_TO_TICKS(100)) != pdTRUE) {
        ESP_LOGW(TAG, "inbound ring buffer full; dropping %u-byte message", (unsigned)data_len);
        return;
    }
    item->kind = (uint8_t)kind;
    item->pad = 0;
    item->len = (uint16_t)data_len;
    if (data_len > 0) {
        memcpy((uint8_t *)item + sizeof(*item), data, data_len);
    }
    if (xRingbufferSendComplete(impl->inbound_rb, item) == pdTRUE) {
        impl->wake_core(); // drain the queue immediately (event-driven)
    } else {
        ESP_LOGE(TAG, "inbound ring buffer commit failed");
    }
}

void soulcloud::soulcloud_client::dispatch_command(const uint8_t *payload, size_t len)
{
    // Commands are paused during OTA, but never silently dropped: the
    // broker has already PUBACKed the message and would otherwise wait
    // forever for a result (default delivery timeout = never). Answer
    // with a busy result so the platform can act explicitly.
    if (soulcloud::ota_executor::instance().is_active()) {
        uint8_t id[16] = {};
        uint64_t seq = 0;
        if (decode_command_id(payload, len, id, &seq) == ERR_OK) {
            command_result result = {};
            result.id = id;
            result.seq = seq;
            result.args = nullptr;
            result.arg_count = 0;
            result.code = soulcloud::command_registry::CMD_RESULT_ERR_BUSY; // -4
            uint8_t buf[1024] = {};
            size_t out_len = 0;
            if (encode_command_result(buf, sizeof(buf), &out_len, &result) == ERR_OK) {
                char topic[160] = {};
                topic_cmd_result(topic, sizeof(topic), _cfg.device_uid);
                impl->bridge.publish(topic, buf, out_len, 1);
            }
        }
        ESP_LOGD(TAG, "command rejected: OTA in progress (busy result)");
        return;
    }
    uint8_t result_buf[1024] = {};
    const int32_t n = soulcloud::command_registry::instance().dispatch(payload, len, result_buf, sizeof(result_buf));
    if (n > 0) {
        char topic[160] = {};
        topic_cmd_result(topic, sizeof(topic), _cfg.device_uid);
        impl->bridge.publish(topic, result_buf, (size_t)n, 1);
    } else {
        ESP_LOGW(TAG, "cmd/exec dispatch failed (%ld)", (long)n);
    }
}

void soulcloud::soulcloud_client::handle_ota_notice(const uint8_t *payload, size_t len)
{
    ota_notice notice;
    const int32_t rc = decode_ota_notice(payload, len, &notice);
    if (rc != ERR_OK) {
        ESP_LOGW(TAG, "decode ota notice failed (%ld)", (long)rc);
        return;
    }
    const esp_err_t err = soulcloud::ota_executor::instance().start(&notice);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "ota start failed: %s", esp_err_to_name(err));
    }
}

void soulcloud::soulcloud_client::notify_wifi_connected()
{
    if (impl != nullptr) {
        impl->bridge.notify_wifi_connected();
    }
}
