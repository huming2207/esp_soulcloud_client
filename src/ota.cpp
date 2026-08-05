/*
 * OTA executor: HTTP download + SHA-256 verify + flash + report + restart.
 *
 * Failure codes follow the backend contract (ota-result.ts):
 *   -1 download failed, -2 checksum mismatch, -3 flash failed,
 *   -4 invalid image, -5 other.
 */

#include "ota.hpp"

#include <cstdio>
#include <cstring>

#include <esp_app_desc.h>
#include <esp_http_client.h>
#include <esp_log.h>
#include <esp_ota_ops.h>
#include <esp_partition.h>
#include <mbedtls/md.h>
#include <nvs.h>

#include "mqtt_bridge.hpp"
#include "protocol.hpp"

using namespace soulcloud;

    static int hex_val(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return 0;
}

esp_err_t soulcloud::ota_executor::init(const config *cfg, mqtt_bridge *bridge)
{
    if (cfg_ != nullptr) {
        return ESP_ERR_INVALID_STATE;
    }
    cfg_ = cfg;
    bridge_ = bridge;
    return ESP_OK;
}

void soulcloud::ota_executor::deinit()
{
    cfg_ = nullptr;
    bridge_ = nullptr;
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

void soulcloud::ota_executor::store_last_ota(const char *release_id)
{
    nvs_handle_t h = 0;
    if (nvs_open("soulcloud", NVS_READWRITE, &h) != ESP_OK) {
        return;
    }
    if (nvs_set_str(h, NVS_KEY_LAST_REL, release_id) == ESP_OK) {
        nvs_commit(h);
    }
    nvs_close(h);
}

esp_err_t soulcloud::ota_executor::start(const ota_notice *notice)
{
    if (cfg_ == nullptr || bridge_ == nullptr || active_) {
        return ESP_ERR_INVALID_STATE;
    }

    // dedupe: this release is already running
    if (last_ota_matches(notice->release_id)) {
        ESP_LOGI(TAG, "release %s already applied; ignoring notice", notice->release_id);
        return ESP_OK;
    }

    // sanity: refuse absurd image sizes (partition guard)
    if (notice->bin_size > cfg_->ota_max_bytes) {
        ESP_LOGE(TAG, "bin_size %u exceeds limit %lu", notice->bin_size,
                 (unsigned long)cfg_->ota_max_bytes);
        return ESP_ERR_INVALID_SIZE;
    }

    snprintf(pending_.release_id, sizeof(pending_.release_id), "%s", notice->release_id);
    snprintf(pending_.job_id, sizeof(pending_.job_id), "%s", notice->job_id);
    snprintf(pending_.bin_sha256, sizeof(pending_.bin_sha256), "%s", notice->bin_sha256);
    pending_.bin_size = notice->bin_size;
    snprintf(pending_.download_url, sizeof(pending_.download_url), "%s", notice->download_url);
    snprintf(pending_.download_token, sizeof(pending_.download_token), "%s", notice->download_token);

    active_ = true;
    const BaseType_t created = xTaskCreate(task_trampoline, "soulcloud_ota",
                                           TASK_STACK, this, TASK_PRIORITY, &task_);
    if (created != pdPASS) {
        active_ = false;
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}

void soulcloud::ota_executor::task_trampoline(void *ctx)
{
    static_cast<ota_executor *>(ctx)->run();
}

void soulcloud::ota_executor::report_state(const char *state, int32_t code, const char *message)
{
    if (bridge_ == nullptr) {
        return;
    }
    uint8_t buf[1024] = {};
    size_t len = 0;
    const ota_result result = {
        .release_id = pending_.release_id,
        .job_id = pending_.job_id,
        .state = state,
        .code = code,
        .message = message,  // NULL -> omitted (backend rejects null)
    };
    if (encode_ota_result(buf, sizeof(buf), &len, &result) != ERR_OK) {
        ESP_LOGE(TAG, "encode ota/result failed");
        return;
    }
    char topic[160] = {};
    topic_ota_result(topic, sizeof(topic), cfg_->device_uid);
    bridge_->publish(topic, buf, len, 1);
}

/**
 * Downloads the image, streams it into the next OTA partition and
 * verifies the SHA-256 against the notice.
 *
 * On success the caller owns *ota_handle and *partition (still open for
 * esp_ota_end); on failure all resources are released and *fail is set.
 */
bool soulcloud::ota_executor::download_and_verify(esp_ota_handle_t *ota_handle_out,
                                       const esp_partition_t **partition_out,
                                       size_t *total_out, ota_fail *fail)
{
    char url[512] = {};
    snprintf(url, sizeof(url), "%s%s", cfg_->api_base_url, pending_.download_url);

    esp_http_client_config_t http_cfg = {};
    http_cfg.url = url;
    http_cfg.method = HTTP_METHOD_GET;
    http_cfg.timeout_ms = (int)cfg_->ota_timeout_s * 1000;
    http_cfg.buffer_size = (int)CHUNK;
    http_cfg.user_agent = "soulcloud-esp32";
    http_cfg.disable_auto_redirect = true;

    esp_http_client_handle_t http = esp_http_client_init(&http_cfg);
    if (http == nullptr) {
        *fail = {-5, "http client init failed"};
        return false;
    }

    char auth[544] = {};
    snprintf(auth, sizeof(auth), "Bearer %s", pending_.download_token);
    esp_http_client_set_header(http, "Authorization", auth);

    if (esp_http_client_open(http, 0) != ESP_OK) {
        esp_http_client_cleanup(http);
        *fail = {-1, "download failed"};
        return false;
    }
    // fetch_headers returns content-length (or 0 if chunked), NOT the HTTP
    // status code; read the status separately before checking it.
    esp_http_client_fetch_headers(http);
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

    mbedtls_md_context_t sha;
    mbedtls_md_init(&sha);
    int mrc = mbedtls_md_setup(&sha, mbedtls_md_info_from_type(MBEDTLS_MD_SHA256), 0);
    if (mrc == 0) {
        mrc = mbedtls_md_starts(&sha);
    }
    if (mrc != 0) {
        ESP_LOGE(TAG, "sha256 setup failed: -0x%x", -mrc);
        esp_ota_abort(ota_handle);
        esp_http_client_cleanup(http);
        mbedtls_md_free(&sha);
        *fail = {-5, "sha256 init failed"};
        return false;
    }

    uint8_t chunk[CHUNK];
    size_t total = 0;
    bool stream_ok = true;
    for (;;) {
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
        if (total > cfg_->ota_max_bytes) {
            ESP_LOGE(TAG, "image exceeds size limit");
            stream_ok = false;
            break;
        }
        if (esp_ota_write(ota_handle, chunk, (size_t)n) != ESP_OK) {
            ESP_LOGE(TAG, "esp_ota_write failed");
            stream_ok = false;
            break;
        }
        mbedtls_md_update(&sha, chunk, (size_t)n);
    }

    if (!stream_ok) {
        esp_ota_abort(ota_handle);
        esp_http_client_cleanup(http);
        mbedtls_md_free(&sha);
        *fail = {-1, "download failed"};
        return false;
    }

    uint8_t digest[32] = {};
    mbedtls_md_finish(&sha, digest);
    mbedtls_md_free(&sha);
    esp_http_client_cleanup(http);

    bool match = true;
    for (size_t i = 0; i < 32; ++i) {
        const int hi = hex_val(pending_.bin_sha256[i * 2]);
        const int lo = hex_val(pending_.bin_sha256[i * 2 + 1]);
        if ((uint8_t)((hi << 4) | lo) != digest[i]) {
            match = false;
        }
    }
    if (!match) {
        ESP_LOGE(TAG, "sha256 mismatch (got %02x%02x..., expected %.16s...)",
                 digest[0], digest[1], pending_.bin_sha256);
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
    ESP_LOGI(TAG, "OTA start: release %s, %u bytes",
             pending_.release_id, pending_.bin_size);

    esp_ota_handle_t ota_handle = 0;
    const esp_partition_t *partition = nullptr;
    size_t total = 0;
    ota_fail fail = {};

    if (!download_and_verify(&ota_handle, &partition, &total, &fail)) {
        ESP_LOGE(TAG, "OTA failed: %s", fail.msg);
        report_state("failed", fail.code, fail.msg);
        active_ = false;
        task_ = nullptr;
        vTaskDelete(nullptr);
        return;
    }
    ESP_LOGI(TAG, "sha256 verified (%zu bytes)", total);
    report_state("downloaded", 0, nullptr);

    esp_err_t err = esp_ota_end(ota_handle);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "esp_ota_end failed: %s", esp_err_to_name(err));
        report_state("failed", err == ESP_ERR_OTA_VALIDATE_FAILED ? -4 : -3, "image invalid");
        active_ = false;
        task_ = nullptr;
        vTaskDelete(nullptr);
        return;
    }

    err = esp_ota_set_boot_partition(partition);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "esp_ota_set_boot_partition failed: %s", esp_err_to_name(err));
        report_state("failed", -3, "boot switch failed");
        active_ = false;
        task_ = nullptr;
        vTaskDelete(nullptr);
        return;
    }

    store_last_ota(pending_.release_id);
    report_state("installed", 0, nullptr);
    ESP_LOGI(TAG, "OTA installed; restarting");

    // let the QoS1 result flush before the restart
    vTaskDelay(pdMS_TO_TICKS(2000));
    esp_restart();
}
