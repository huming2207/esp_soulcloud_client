#pragma once

/*
 * OTA executor (singleton): downloads a release bin over HTTP, verifies
 * its SHA-256, streams it into the next OTA partition, reports
 * downloaded/installed/failed via ota/result and restarts.
 *
 * Runs in a dedicated task so the MQTT event loop is never blocked.
 * Deduplication: the release id of the last successful OTA is kept in NVS
 * so redelivered notices are ignored.
 */

#include <atomic>
#include <cstddef>
#include <cstdint>

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

#include <esp_ota_ops.h>
#include <esp_partition.h>
#include <mbedtls/md.h>

#include "soulcloud.hpp"

namespace soulcloud
{
    class mqtt_bridge;
    class ota_notice;

    /**
     * @brief OTA executor (singleton).
     *
     * One OTA flow at a time: init() creates a persistent worker; start()
     * copies the notice into internal storage and wakes that worker to
     * download, verify, flash and restart. Commands are rejected as busy while active
     * (soulcloud_client checks is_active()).
     */
    class ota_executor
    {
    public:
        /**
         * @brief Access the singleton instance.
         * @return Reference to the process-wide executor.
         */
        static ota_executor &instance()
        {
            static ota_executor s_instance;
            return s_instance;
        }

        ota_executor(const ota_executor &) = delete;
        ota_executor &operator=(const ota_executor &) = delete;

        /**
         * @brief Configure the executor.
         *
         * @param[in] cfg    Configuration (BORROWED: must outlive the
         *                   executor — the client passes its own copy).
         * @param[in] bridge MQTT bridge (borrowed, must outlive the
         *                   executor; used for ota/result reports).
         * @return ESP_OK, or ESP_ERR_INVALID_STATE if already init'd.
         */
        esp_err_t init(const config *cfg, mqtt_bridge *bridge);

        /**
         * @brief Release cfg/bridge (idempotent).
         *
         * Waits (bounded) for an in-flight OTA operation to finish before
         * releasing the pointers it dereferences: the task reports its
         * state through the bridge and reads the config on every exit
         * path, so freeing them under it used to be a NULL deref. If the
         * worker is still active after the wait (e.g. a stuck download), it
         * is force-deleted as a last resort.
         */
        void deinit();

        /**
         * @brief First-boot self-check: promote the pending OTA release.
         *
         * Called once the device reaches a healthy state (first MQTT
         * connect). When a release was installed and the device
         * restarted, the release id is only a *pending* candidate in NVS;
         * this call cancels the bootloader rollback (rollback-enabled
         * builds) and promotes it to the dedupe key. If the new firmware
         * never boots far enough to connect, the bootloader rolls back
         * and the old firmware never saw the release as applied, so a
         * redelivered notice triggers a fresh OTA instead of being
         * ignored forever.
         *
         * Idempotent: after a successful promotion the pending key is
         * gone. Safe to call on every connect.
         */
        void finalize_pending_ota();

        /** @brief True while an OTA download/flash is in progress. */
        bool is_active() const
        {
            return active.load(std::memory_order_acquire);
        }

        /**
         * @brief Start the OTA flow for a validated notice.
         *
         * Copies the notice and wakes the init-created OTA task. Refuses to start
         * while another OTA is active; a notice for a release that is
         * already running (NVS dedupe) is acknowledged with an
         * "installed" result for the notice's job instead of being
         * silently ignored.
         *
         * @param[in] notice Decoded OTA notice.
         * @return
         *  - ESP_OK
         *  - ESP_ERR_INVALID_STATE if not init'd or already active
         *  - ESP_ERR_INVALID_SIZE   if bin_size exceeds cfg->ota_max_bytes
         */
        esp_err_t start(const ota_notice *notice);

    private:
        ota_executor() = default;

        static constexpr char TAG[] = "soulcloud_ota";
        static constexpr uint32_t TASK_STACK = 10240;
        static constexpr uint32_t TASK_PRIORITY = 5;
        static constexpr size_t CHUNK = 4096;
        static constexpr char NVS_KEY_LAST_REL[] = "ota_rel";
        static constexpr char NVS_KEY_PENDING_REL[] = "ota_pend";
        static constexpr char NVS_KEY_PENDING_ADDR[] = "ota_paddr";

        struct ota_fail {
            int32_t code;
            const char *msg;
        };

        const config *_cfg = nullptr;
        mqtt_bridge *_bridge = nullptr;
        // cross-task lifecycle state (ESP-R6): atomics with documented
        // ownership — init/deinit own the persistent worker handle; the
        // worker clears active after each operation
        std::atomic<TaskHandle_t> task{nullptr};
        std::atomic<bool> active{false};
        std::atomic<bool> exit_requested{false};
        std::atomic<bool> worker_exited{false};
        mbedtls_md_context_t sha_ctx = {};
        bool sha_ready = false;

        // the notice under execution (single in-flight OTA at a time)
        struct notice_copy {
            char release_id[37];
            char job_id[37];
            char bin_sha256[65];
            uint32_t bin_size;
            char download_url[192];
            char download_token[512];
        };
        notice_copy pending = {};

        void run();
        void run_once();
        bool download_and_verify(esp_ota_handle_t *ota_handle_out, const esp_partition_t **partition_out, size_t *total_out,
                                 ota_fail *fail);
        void report_state(const char *state, int32_t code, const char *message);
        bool last_ota_matches(const char *release_id) const;
        esp_err_t store_pending_ota(const char *release_id, uint32_t partition_address);

        static void task_trampoline(void *ctx);
    };
} // namespace soulcloud
