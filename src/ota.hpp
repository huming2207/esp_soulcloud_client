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

#include <cstddef>
#include <cstdint>

#include <freertos/FreeRTOS.h>
#include <freertos/task.h>

#include <esp_ota_ops.h>
#include <esp_partition.h>

#include "soulcloud.hpp"

namespace soulcloud
{
    class mqtt_bridge;
    class ota_notice;

    class ota_executor
    {
    public:
        static ota_executor &instance()
        {
            static ota_executor s_instance;
            return s_instance;
        }

        ota_executor(const ota_executor &) = delete;
        ota_executor &operator=(const ota_executor &) = delete;

        esp_err_t init(const config *cfg, mqtt_bridge *bridge);
        void deinit();

        /** True while an OTA download/flash is in progress. */
        bool is_active() const { return active_; }

        /** Starts the OTA flow for a validated notice (copied). */
        esp_err_t start(const ota_notice *notice);

    private:
        ota_executor() = default;

        static constexpr char TAG[] = "soulcloud_ota";
        static constexpr uint32_t TASK_STACK = 10240;
        static constexpr uint32_t TASK_PRIORITY = 5;
        static constexpr size_t CHUNK = 4096;
        static constexpr char NVS_KEY_LAST_REL[] = "ota_rel";

        struct ota_fail
        {
            int32_t code;
            const char *msg;
        };

        const config *cfg_ = nullptr;
        mqtt_bridge *bridge_ = nullptr;
        TaskHandle_t task_ = nullptr;
        volatile bool active_ = false;

        // the notice under execution (single in-flight OTA at a time)
        struct notice_copy
        {
            char release_id[37];
            char job_id[37];
            char bin_sha256[65];
            uint32_t bin_size;
            char download_url[192];
            char download_token[512];
        };
        notice_copy pending_ = {};

        void run();
        bool download_and_verify(esp_ota_handle_t *ota_handle_out,
                                 const esp_partition_t **partition_out,
                                 size_t *total_out, ota_fail *fail);
        void report_state(const char *state, int32_t code, const char *message);
        bool last_ota_matches(const char *release_id) const;
        void store_last_ota(const char *release_id);

        static void task_trampoline(void *ctx);
    };
}  // namespace soulcloud
