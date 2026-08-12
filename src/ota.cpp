/*
 * OTA executor: HTTP download + SHA-256 verify + flash + report + restart.
 *
 * Failure codes follow the backend contract (ota-result.ts):
 *   -1 download failed, -2 checksum mismatch, -3 flash failed,
 *   -4 invalid image, -5 other.
 */

#include "ota.hpp"

#include <sdkconfig.h>

#include <climits>
#include <cstdio>
#include <cstring>

#include <esp_app_desc.h>
#include <esp_http_client.h>
#include <esp_log.h>
#include <esp_ota_ops.h>
#include <esp_partition.h>
#include <esp_timer.h>
#include <nvs.h>
#ifdef CONFIG_MBEDTLS_CERTIFICATE_BUNDLE
#include <esp_crt_bundle.h>
#endif

#include "mqtt_bridge.hpp"
#include "protocol.hpp"

using namespace soulcloud;

static int hex_val(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return 0;
}

static inline int remaining_timeout_ms(uint64_t deadline_us)
{
    const uint64_t now = (uint64_t)esp_timer_get_time();
    if (now >= deadline_us) {
        return 0;
    }
    const uint64_t remaining_ms = (deadline_us - now + 999u) / 1000u;
    return remaining_ms > (uint64_t)INT_MAX ? INT_MAX : (int)remaining_ms;
}

esp_err_t soulcloud::ota_executor::init(const config *cfg, mqtt_bridge *bridge)
{
    if (_cfg != nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    _cfg = cfg;
    _bridge = bridge;
    active.store(false, std::memory_order_release);
    exit_requested.store(false, std::memory_order_release);
    worker_exited.store(false, std::memory_order_release);

    // mbedtls_md_setup allocates its algorithm context. Do it once here,
    // never in the OTA hot path, and reuse the context for every attempt.
    mbedtls_md_init(&sha_ctx);
    int mrc = mbedtls_md_setup(&sha_ctx, mbedtls_md_info_from_type(MBEDTLS_MD_SHA256), 0);
    if (mrc != 0) {
        mbedtls_md_free(&sha_ctx);
        _cfg = nullptr;
        _bridge = nullptr;
        return ESP_ERR_NO_MEM;
    }
    sha_ready = true;

    TaskHandle_t created_task = nullptr;
    if (xTaskCreate(task_trampoline, "soulcloud_ota", TASK_STACK, this, TASK_PRIORITY, &created_task) != pdPASS) {
        mbedtls_md_free(&sha_ctx);
        sha_ready = false;
        _cfg = nullptr;
        _bridge = nullptr;
        return ESP_ERR_NO_MEM;
    }
    // The worker starts idle and remains allocated until deinit(), so OTA
    // retries do not allocate/free task stacks in steady state.
    task.store(created_task, std::memory_order_release);
    return ESP_OK;
}

void soulcloud::ota_executor::deinit()
{
    // An in-flight OTA task dereferences _cfg/_bridge on every state
    // report and exit path; wait for it (bounded) before releasing them.
    // The download itself is bounded by the HTTP timeout, so a live task
    // normally ends within ota_timeout_s; the forced delete below is a
    // last resort that leaks the HTTP/OTA handles but is safe against
    // NULL derefs (and deinit is a terminating operation anyway).
    // Ask the persistent worker to leave its wait loop. While an OTA is in
    // progress, the notification remains pending until run_once() returns.
    const TaskHandle_t h = task.load(std::memory_order_acquire);
    if (h != nullptr) {
        exit_requested.store(true, std::memory_order_release);
        xTaskNotifyGive(h);
        const uint32_t budget_ms = 1000u + (uint32_t)(_cfg != nullptr ? _cfg->ota_timeout_s : 0) * 1000u;
        const TickType_t budget_ticks = pdMS_TO_TICKS(budget_ms);
        TickType_t waited = 0;
        while (!worker_exited.load(std::memory_order_acquire) && waited < budget_ticks) {
            vTaskDelay(1);
            waited++;
        }
        if (!worker_exited.load(std::memory_order_acquire)) {
            ESP_LOGW(TAG, "OTA worker still running after %lu ms; force-deleting", (unsigned long)budget_ms);
        }
        // The worker parks after publishing worker_exited, keeping the
        // handle live until this owner deletes it.
        vTaskDelete(h);
        task.store(nullptr, std::memory_order_release);
    }
    active.store(false, std::memory_order_release);
    exit_requested.store(false, std::memory_order_release);
    worker_exited.store(false, std::memory_order_release);
    if (sha_ready) {
        mbedtls_md_free(&sha_ctx);
        sha_ready = false;
    }
    _cfg = nullptr;
    _bridge = nullptr;
}

bool soulcloud::ota_executor::last_ota_matches(const char *release_id) const
{
    nvs_handle_t h = 0;
    if (nvs_open("soulcloud", NVS_READONLY, &h) != ESP_OK) {
        return false;
    }
    char last[37] = {};
    size_t len = sizeof(last);
    const esp_err_t err = nvs_get_str(h, NVS_KEY_LAST_REL, last, &len);
    nvs_close(h);
    return err == ESP_OK && strcmp(last, release_id) == 0;
}

esp_err_t soulcloud::ota_executor::store_pending_ota(const char *release_id, uint32_t partition_address)
{
    nvs_handle_t h = 0;
    esp_err_t err = nvs_open("soulcloud", NVS_READWRITE, &h);
    if (err != ESP_OK) {
        return err;
    }
    err = nvs_set_str(h, NVS_KEY_PENDING_REL, release_id);
    if (err == ESP_OK) {
        err = nvs_set_u32(h, NVS_KEY_PENDING_ADDR, partition_address);
    }
    if (err == ESP_OK) {
        err = nvs_commit(h);
    }
    nvs_close(h);
    return err;
}

void soulcloud::ota_executor::finalize_pending_ota()
{
    nvs_handle_t h = 0;
    if (nvs_open("soulcloud", NVS_READWRITE, &h) != ESP_OK) {
        return;
    }
    char pending_rel[37] = {};
    size_t len = sizeof(pending_rel);
    if (nvs_get_str(h, NVS_KEY_PENDING_REL, pending_rel, &len) != ESP_OK) {
        nvs_close(h);
        return; // nothing pending (also the common case)
    }
    uint32_t pending_addr = 0;
    const esp_err_t addr_err = nvs_get_u32(h, NVS_KEY_PENDING_ADDR, &pending_addr);
    const bool have_pending_addr = addr_err == ESP_OK;
    if (addr_err != ESP_OK && addr_err != ESP_ERR_NVS_NOT_FOUND) {
        ESP_LOGW(TAG, "read pending OTA partition failed: %s", esp_err_to_name(addr_err));
        nvs_close(h);
        return;
    }
    // First boot after an OTA: only the freshly flashed image is allowed
    // to promote the pending release. With rollback enabled the bootloader
    // marks it PENDING_VERIFY; any other state means we booted an image
    // that was NOT just flashed (e.g. the new firmware crashed and the
    // bootloader rolled back to the old one). Promoting then would record
    // the failed release as applied forever, so drop the pending record
    // instead.
    const esp_partition_t *running = esp_ota_get_running_partition();
    esp_ota_img_states_t state = ESP_OTA_IMG_UNDEFINED;
    if (running != nullptr) {
        esp_ota_get_state_partition(running, &state);
    }
    // New records include the exact target partition. This distinguishes a
    // successful validation retry (same partition, already VALID) from a
    // rollback to the old image (different partition). Records written by
    // older component versions have no address and keep the legacy
    // PENDING_VERIFY-only handling below.
    if (have_pending_addr && (running == nullptr || running->address != pending_addr)) {
        ESP_LOGW(TAG, "running partition does not match pending release %s; dropping it", pending_rel);
        nvs_erase_key(h, NVS_KEY_PENDING_REL);
        nvs_erase_key(h, NVS_KEY_PENDING_ADDR);
        nvs_commit(h);
        nvs_close(h);
        return;
    }
#if CONFIG_BOOTLOADER_APP_ROLLBACK_ENABLE
    if ((!have_pending_addr && state != ESP_OTA_IMG_PENDING_VERIFY) ||
        (have_pending_addr && state != ESP_OTA_IMG_PENDING_VERIFY && state != ESP_OTA_IMG_VALID)) {
        ESP_LOGW(TAG,
                 "booted image is not pending-verify (state %d); "
                 "dropping pending release %s",
                 (int)state, pending_rel);
        nvs_erase_key(h, NVS_KEY_PENDING_REL);
        nvs_erase_key(h, NVS_KEY_PENDING_ADDR);
        nvs_commit(h);
        nvs_close(h);
        return;
    }
    if (state == ESP_OTA_IMG_PENDING_VERIFY) {
        if (esp_ota_mark_app_valid_cancel_rollback() != ESP_OK) {
            ESP_LOGW(TAG, "mark app valid failed; keeping release pending");
            nvs_close(h);
            return; // retried on the next connect
        }
    }
#else
    (void)state; // no rollback mechanism: the pending image is the running one
#endif
    // Promote atomically. If persistence fails, keep the pending keys so a
    // same-partition VALID boot can retry instead of redownloading the image.
    esp_err_t err = nvs_set_str(h, NVS_KEY_LAST_REL, pending_rel);
    if (err == ESP_OK) {
        err = nvs_erase_key(h, NVS_KEY_PENDING_REL);
    }
    if (err == ESP_OK) {
        err = nvs_erase_key(h, NVS_KEY_PENDING_ADDR);
        if (err == ESP_ERR_NVS_NOT_FOUND) {
            err = ESP_OK; // legacy record written before partition tracking
        }
    }
    if (err == ESP_OK) {
        err = nvs_commit(h);
    }
    nvs_close(h);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "promote pending OTA failed: %s", esp_err_to_name(err));
        return;
    }
    ESP_LOGI(TAG, "release %s validated on first boot", pending_rel);
}

esp_err_t soulcloud::ota_executor::start(const ota_notice *notice)
{
    if (_cfg == nullptr || _bridge == nullptr || task.load(std::memory_order_acquire) == nullptr ||
        active.load(std::memory_order_acquire)) {
        return ESP_ERR_INVALID_STATE;
    }

    // dedupe: this release is already running. The backend mints a fresh
    // job/target per deploy, so the notice's job must still be
    // acknowledged or the target stalls and fails even though the
    // desired firmware is running.
    if (last_ota_matches(notice->release_id)) {
        ESP_LOGI(TAG, "release %s already applied; acking job %s", notice->release_id, notice->job_id);
        snprintf(pending.release_id, sizeof(pending.release_id), "%s", notice->release_id);
        snprintf(pending.job_id, sizeof(pending.job_id), "%s", notice->job_id);
        report_state("installed", 0, "already applied");
        return ESP_OK;
    }

    // sanity: refuse absurd image sizes (partition guard)
    if (notice->bin_size > _cfg->ota_max_bytes) {
        ESP_LOGE(TAG, "bin_size %u exceeds limit %lu", notice->bin_size, (unsigned long)_cfg->ota_max_bytes);
        return ESP_ERR_INVALID_SIZE;
    }

    snprintf(pending.release_id, sizeof(pending.release_id), "%s", notice->release_id);
    snprintf(pending.job_id, sizeof(pending.job_id), "%s", notice->job_id);
    snprintf(pending.bin_sha256, sizeof(pending.bin_sha256), "%s", notice->bin_sha256);
    pending.bin_size = notice->bin_size;
    snprintf(pending.download_url, sizeof(pending.download_url), "%s", notice->download_url);
    snprintf(pending.download_token, sizeof(pending.download_token), "%s", notice->download_token);

    active.store(true, std::memory_order_release);
    xTaskNotifyGive(task.load(std::memory_order_acquire));
    return ESP_OK;
}

void soulcloud::ota_executor::task_trampoline(void *ctx)
{
    static_cast<ota_executor *>(ctx)->run();
}

void soulcloud::ota_executor::report_state(const char *state, int32_t code, const char *message)
{
    if (_bridge == nullptr) {
        return;
    }
    uint8_t buf[1024] = {};
    size_t len = 0;
    const ota_result result = {
        .release_id = pending.release_id,
        .job_id = pending.job_id,
        .state = state,
        .code = code,
        .message = message, // NULL -> omitted (backend rejects null)
    };
    if (encode_ota_result(buf, sizeof(buf), &len, &result) != ERR_OK) {
        ESP_LOGE(TAG, "encode ota/result failed");
        return;
    }
    char topic[160] = {};
    topic_ota_result(topic, sizeof(topic), _cfg->device_uid);
    _bridge->publish(topic, buf, len, 1);
}

/**
 * Downloads the image, streams it into the next OTA partition and
 * verifies the SHA-256 against the notice.
 *
 * On success the caller owns *ota_handle and *partition (still open for
 * esp_ota_end); on failure all resources are released and *fail is set.
 */
bool soulcloud::ota_executor::download_and_verify(esp_ota_handle_t *ota_handle_out, const esp_partition_t **partition_out,
                                                  size_t *total_out, ota_fail *fail)
{
    // Enforce one wall-clock budget for the entire request. ESP HTTP's
    // timeout is an individual blocking-I/O timeout; without reducing it
    // to the remaining budget, a peer that keeps making slow progress can
    // extend an OTA indefinitely.
    const uint64_t deadline_us = (uint64_t)esp_timer_get_time() + (uint64_t)_cfg->ota_timeout_s * 1000000ull;
    char url[512] = {};
    snprintf(url, sizeof(url), "%s%s", _cfg->api_base_url, pending.download_url);

    esp_http_client_config_t http_cfg = {};
    http_cfg.url = url;
    http_cfg.method = HTTP_METHOD_GET;
    http_cfg.timeout_ms = (int)_cfg->ota_timeout_s * 1000;
    http_cfg.buffer_size = (int)CHUNK;
    http_cfg.user_agent = "soulcloud-esp32";
    http_cfg.disable_auto_redirect = true;
#ifdef CONFIG_MBEDTLS_CERTIFICATE_BUNDLE
    http_cfg.crt_bundle_attach = esp_crt_bundle_attach; // https:// download
#endif

    esp_http_client_handle_t http = esp_http_client_init(&http_cfg);
    if (http == nullptr) {
        *fail = {-5, "http client init failed"};
        return false;
    }

    char auth[544] = {};
    snprintf(auth, sizeof(auth), "Bearer %s", pending.download_token);
    esp_http_client_set_header(http, "Authorization", auth);

    int timeout_ms = remaining_timeout_ms(deadline_us);
    if (timeout_ms == 0 || esp_http_client_set_timeout_ms(http, timeout_ms) != ESP_OK) {
        esp_http_client_cleanup(http);
        *fail = {-1, "download timed out"};
        return false;
    }
    if (esp_http_client_open(http, 0) != ESP_OK) {
        esp_http_client_cleanup(http);
        *fail = {-1, "download failed"};
        return false;
    }
    // fetch_headers returns content-length (or 0 if chunked), NOT the HTTP
    // status code; read the status separately before checking it.
    timeout_ms = remaining_timeout_ms(deadline_us);
    if (timeout_ms == 0 || esp_http_client_set_timeout_ms(http, timeout_ms) != ESP_OK ||
        esp_http_client_fetch_headers(http) < 0) {
        esp_http_client_cleanup(http);
        *fail = {-1, "download timed out"};
        return false;
    }
    const int status = esp_http_client_get_status_code(http);
    if (status != HttpStatus_Ok) {
        ESP_LOGE(TAG, "download status %d", status);
        esp_http_client_cleanup(http);
        *fail = {-1, "download failed"};
        return false;
    }

    const esp_partition_t *partition = esp_ota_get_next_update_partition(nullptr);
    if (partition == nullptr) {
        esp_http_client_cleanup(http);
        *fail = {-3, "no OTA partition"};
        return false;
    }

    esp_ota_handle_t ota_handle = 0;
    if (esp_ota_begin(partition, OTA_WITH_SEQUENTIAL_WRITES, &ota_handle) != ESP_OK) {
        esp_http_client_cleanup(http);
        *fail = {-3, "flash begin failed"};
        return false;
    }

    // sha_ctx was allocated once in init(); starts() only resets it.
    const int mrc = sha_ready ? mbedtls_md_starts(&sha_ctx) : -1;
    if (mrc != 0) {
        ESP_LOGE(TAG, "sha256 setup failed: -0x%x", -mrc);
        esp_ota_abort(ota_handle);
        esp_http_client_cleanup(http);
        *fail = {-5, "sha256 init failed"};
        return false;
    }

    uint8_t chunk[CHUNK];
    size_t total = 0;
    bool stream_ok = true;
    uint32_t chunks_since_delay = 0;
    for (;;) {
        if (exit_requested.load(std::memory_order_acquire)) {
            *fail = {-5, "OTA cancelled"};
            stream_ok = false;
            break;
        }
        timeout_ms = remaining_timeout_ms(deadline_us);
        if (timeout_ms == 0 || esp_http_client_set_timeout_ms(http, timeout_ms) != ESP_OK) {
            *fail = {-1, "download timed out"};
            stream_ok = false;
            break;
        }
        const int n = esp_http_client_read(http, (char *)chunk, sizeof(chunk));
        if (n < 0) {
            ESP_LOGE(TAG, "read error %d", n);
            stream_ok = false;
            break;
        }
        if (n == 0) {
            if (!esp_http_client_is_complete_data_received(http)) {
                ESP_LOGE(TAG, "connection closed before complete data");
                stream_ok = false;
            }
            break;
        }
        total += (size_t)n;
        if (total > _cfg->ota_max_bytes) {
            ESP_LOGE(TAG, "image exceeds size limit");
            stream_ok = false;
            break;
        }
        if (esp_ota_write(ota_handle, chunk, (size_t)n) != ESP_OK) {
            ESP_LOGE(TAG, "esp_ota_write failed");
            stream_ok = false;
            break;
        }
        if (mbedtls_md_update(&sha_ctx, chunk, (size_t)n) != 0) {
            ESP_LOGE(TAG, "sha256 update failed");
            stream_ok = false;
            break;
        }
        if (++chunks_since_delay == 16) {
            // Fast local transports must still allow idle to feed TWDT.
            vTaskDelay(1);
            chunks_since_delay = 0;
        }
    }

    if (!stream_ok) {
        esp_ota_abort(ota_handle);
        esp_http_client_cleanup(http);
        if (fail->msg == nullptr) {
            *fail = {-1, "download failed"};
        }
        return false;
    }

    // the notice's declared size is authoritative; a mismatch means the
    // server metadata or the transfer was wrong (SHA-256 below still
    // protects integrity, but the size check keeps telemetry honest)
    if (total != pending.bin_size) {
        ESP_LOGE(TAG, "size mismatch: got %zu bytes, notice declared %u", total, pending.bin_size);
        esp_ota_abort(ota_handle);
        esp_http_client_cleanup(http);
        *fail = {-5, "size mismatch"};
        return false;
    }

    uint8_t digest[32] = {};
    if (mbedtls_md_finish(&sha_ctx, digest) != 0) {
        esp_ota_abort(ota_handle);
        esp_http_client_cleanup(http);
        *fail = {-5, "sha256 finish failed"};
        return false;
    }
    esp_http_client_cleanup(http);

    bool match = true;
    for (size_t i = 0; i < 32; ++i) {
        const int hi = hex_val(pending.bin_sha256[i * 2]);
        const int lo = hex_val(pending.bin_sha256[i * 2 + 1]);
        if ((uint8_t)((hi << 4) | lo) != digest[i]) {
            match = false;
        }
    }
    if (!match) {
        ESP_LOGE(TAG, "sha256 mismatch (got %02x%02x..., expected %.16s...)", digest[0], digest[1], pending.bin_sha256);
        esp_ota_abort(ota_handle);
        *fail = {-2, "checksum mismatch"};
        return false;
    }

    *ota_handle_out = ota_handle;
    *partition_out = partition;
    *total_out = total;
    return true;
}

void soulcloud::ota_executor::run()
{
    for (;;) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
        if (exit_requested.load(std::memory_order_acquire)) {
            break;
        }
        run_once();
        active.store(false, std::memory_order_release);
    }

    // Publish completion but keep the task handle live. deinit() owns and
    // deletes this init-created worker, avoiding a stale-handle race. Do
    // not park on a notification: a concurrent start() notification could
    // otherwise let the task return and invalidate the stored handle.
    worker_exited.store(true, std::memory_order_release);
    vTaskSuspend(nullptr);
}

void soulcloud::ota_executor::run_once()
{
    ESP_LOGI(TAG, "OTA start: release %s, %u bytes", pending.release_id, pending.bin_size);

    esp_ota_handle_t ota_handle = 0;
    const esp_partition_t *partition = nullptr;
    size_t total = 0;
    ota_fail fail = {};

    if (!download_and_verify(&ota_handle, &partition, &total, &fail)) {
        ESP_LOGE(TAG, "OTA failed: %s", fail.msg);
        report_state("failed", fail.code, fail.msg);
        return;
    }
    ESP_LOGI(TAG, "sha256 verified (%zu bytes)", total);
    report_state("downloaded", 0, nullptr);

    esp_err_t err = esp_ota_end(ota_handle);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "esp_ota_end failed: %s", esp_err_to_name(err));
        report_state("failed", err == ESP_ERR_OTA_VALIDATE_FAILED ? -4 : -3, "image invalid");
        return;
    }

    err = esp_ota_set_boot_partition(partition);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "esp_ota_set_boot_partition failed: %s", esp_err_to_name(err));
        report_state("failed", -3, "boot switch failed");
        return;
    }

    // The release id is only *pending* until the new firmware's first
    // successful boot validates it (finalize_pending_ota on first MQTT
    // connect): writing the dedupe key before the restart would mark a
    // release as applied even when the bootloader rolls the new image
    // back, and the release could never be re-delivered.
    err = store_pending_ota(pending.release_id, partition->address);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "store pending OTA failed: %s", esp_err_to_name(err));
        const esp_partition_t *running = esp_ota_get_running_partition();
        if (running == nullptr || esp_ota_set_boot_partition(running) != ESP_OK) {
            ESP_LOGE(TAG, "failed to restore current boot partition");
        }
        report_state("failed", -3, "OTA state persist failed");
        return;
    }
    report_state("installed", 0, nullptr);
    ESP_LOGI(TAG, "OTA installed; restarting");

    // let the QoS1 result flush before the restart
    vTaskDelay(pdMS_TO_TICKS(2000));
    esp_restart();
}
