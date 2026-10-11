#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

#include <esp_err.h>
#include <esp_http_client.h>
#include <esp_ota_ops.h>
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <psa/crypto.h>

#include "sc_messages.hpp"

class sc_mqtt;

/**
 * Firmware updates pulled over HTTPS from the signed URL in ota.start.
 *
 * The download runs on its own task (created per update, deleted when it
 * ends) and writes straight into the next OTA partition while hashing the
 * stream; the SHA-256 must match the one the platform sent. A broken
 * connection resumes with an HTTP Range request. Progress goes to
 * d/<id>/up/evt/ota; on success the device switches the boot partition and
 * restarts, and the platform completes the update when the new firmware
 * reports in.
 *
 * Memory: the task stack and a CONFIG_SOULCLOUD_OTA_HTTP_BUFFER read buffer
 * exist only during an update.
 */
class sc_ota
{
public:
    esp_err_t init(sc_mqtt *_mqtt);

    /** Start an update. ESP_OK if this update is already running, ESP_ERR_INVALID_STATE for a different one. */
    esp_err_t start(const sc_ota_params &_params);

    /** Stop the running update if its task ID is @p task_id. */
    esp_err_t cancel(const char *task_id);

    bool busy() const
    {
        return running.load();
    }

private:
    static void task_handler(void *ctx);
    esp_err_t run();
    esp_err_t download(esp_ota_handle_t handle, uint8_t *sha256_out);
    /** Read from @p offset onwards into the OTA handle. Returns bytes read, or -1 on a connection error. */
    int download_from(esp_ota_handle_t handle, size_t offset, psa_hash_operation_t *hash, uint8_t *buf);
    void report(const char *state, int progress, const char *error);

    sc_mqtt *mqtt = nullptr;
    sc_ota_params params = {};
    std::atomic<bool> running = false;
    std::atomic<bool> cancel_requested = false;
    int last_progress = -1;
    int64_t last_report_us = 0;

    static const constexpr int MAX_ATTEMPTS = 5;
    static const constexpr int HTTP_TIMEOUT_MS = 15000;
    static const constexpr int64_t REPORT_INTERVAL_US = 2000000;
    static const constexpr uint32_t RESTART_DELAY_MS = 1500;
    static const constexpr char *TAG = "sc_ota";
};
