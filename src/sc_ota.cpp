#include "sc_ota.hpp"

#include <cstdio>
#include <cstring>

#include <esp_crt_bundle.h>
#include <esp_heap_caps.h>
#include <esp_log.h>
#include <esp_system.h>
#include <esp_timer.h>

#include "sc_codec.hpp"
#include "sc_mqtt.hpp"

esp_err_t sc_ota::init(sc_mqtt *_mqtt)
{
    mqtt = _mqtt;
    return ESP_OK;
}

esp_err_t sc_ota::start(const sc_ota_params &_params)
{
    bool expected = false;
    if (!running.compare_exchange_strong(expected, true)) {
        // The same update can arrive twice (ota.check answer and ota.start request): not an error.
        return strcmp(_params.task_id, params.task_id) == 0 ? ESP_OK : ESP_ERR_INVALID_STATE;
    }

    params = _params;
    cancel_requested.store(false);
    last_progress = -1;
    last_report_us = 0;

    // Flash is written from this task, so its stack must be in internal RAM.
    if (xTaskCreate(task_handler, "sc_ota", CONFIG_SOULCLOUD_OTA_TASK_STACK, this, tskIDLE_PRIORITY + 3, nullptr) != pdPASS) {
        running.store(false);
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}

esp_err_t sc_ota::cancel(const char *task_id)
{
    if (!running.load() || strcmp(task_id, params.task_id) != 0) {
        return ESP_ERR_NOT_FOUND;
    }
    cancel_requested.store(true);
    return ESP_OK;
}

void sc_ota::report(const char *state, int progress, const char *error)
{
    sc_ota_event evt = {};
    evt.task_id = params.task_id;
    evt.state = state;
    evt.progress = progress;
    evt.error = error;

    char body[256] = {};
    size_t len = 0;
    sc_publish_props props = {};
    props.content_type = sc_codec::CONTENT_TYPE;
    if (sc_codec::encode_ota_event(evt, body, sizeof(body), &len) == ESP_OK) {
        mqtt->publish("evt/ota", body, len, 1, &props);
    }
}

void sc_ota::task_handler(void *ctx)
{
    auto *ota = static_cast<sc_ota *>(ctx);
    auto ret = ota->run();
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "update %s failed: 0x%x %s", ota->params.task_id, ret, esp_err_to_name(ret));
    }
    ota->running.store(false);
    vTaskDelete(nullptr);
}

esp_err_t sc_ota::run()
{
    const esp_partition_t *partition = esp_ota_get_next_update_partition(nullptr);
    if (partition == nullptr || params.size > partition->size) {
        report("failed", -1, partition == nullptr ? "no OTA partition" : "image larger than the OTA partition");
        return ESP_ERR_INVALID_SIZE;
    }

    ESP_LOGI(TAG, "updating to %s (%lu bytes) into %s", params.version, static_cast<unsigned long>(params.size), partition->label);
    report("downloading", 0, nullptr);

    esp_ota_handle_t handle = 0;
    auto ret = esp_ota_begin(partition, params.size, &handle);
    if (ret != ESP_OK) {
        report("failed", -1, esp_err_to_name(ret));
        return ret;
    }

    uint8_t sha256[32] = {};
    ret = download(handle, sha256);
    if (ret != ESP_OK) {
        esp_ota_abort(handle);
        report("failed", -1, cancel_requested.load() ? "cancelled" : esp_err_to_name(ret));
        return ret;
    }

    report("verifying", 100, nullptr);
    char sha256_hex[65] = {};
    for (int idx = 0; idx < 32; idx++) {
        snprintf(sha256_hex + idx * 2, 3, "%02x", sha256[idx]);
    }
    if (strcmp(sha256_hex, params.sha256) != 0) {
        esp_ota_abort(handle);
        report("failed", -1, "SHA-256 mismatch");
        return ESP_ERR_INVALID_CRC;
    }

    // esp_ota_end() also checks the image header, chip and (with secure boot) signature.
    ret = esp_ota_end(handle);
    ret = ret ?: esp_ota_set_boot_partition(partition);
    if (ret != ESP_OK) {
        report("failed", -1, esp_err_to_name(ret));
        return ret;
    }

    report("rebooting", 100, nullptr);
    ESP_LOGW(TAG, "update written, restarting");
    vTaskDelay(pdMS_TO_TICKS(RESTART_DELAY_MS));
    esp_restart();
    return ESP_OK;
}

esp_err_t sc_ota::download(esp_ota_handle_t handle, uint8_t *sha256_out)
{
    auto *buf = static_cast<uint8_t *>(heap_caps_malloc(CONFIG_SOULCLOUD_OTA_HTTP_BUFFER, MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT));
    if (buf == nullptr) {
        return ESP_ERR_NO_MEM;
    }

    psa_hash_operation_t hash = psa_hash_operation_init();
    if (psa_hash_setup(&hash, PSA_ALG_SHA_256) != PSA_SUCCESS) {
        heap_caps_free(buf);
        return ESP_FAIL;
    }

    size_t offset = 0;
    int attempts = 0;
    esp_err_t ret = ESP_OK;
    while (offset < params.size) {
        if (cancel_requested.load()) {
            ret = ESP_ERR_INVALID_STATE;
            break;
        }

        int read_len = download_from(handle, offset, &hash, buf);
        if (read_len > 0) {
            offset += read_len;
            attempts = 0; // Progress was made; start counting failures again
            continue;
        }

        attempts += 1;
        if (read_len < -1 || attempts >= MAX_ATTEMPTS) {
            ret = read_len < -1 ? ESP_ERR_INVALID_RESPONSE : ESP_ERR_TIMEOUT;
            break;
        }
        ESP_LOGW(TAG, "download interrupted at %u, retrying (%d/%d)", offset, attempts, MAX_ATTEMPTS);
        vTaskDelay(pdMS_TO_TICKS(1000 * attempts));
    }

    size_t hash_len = 0;
    if (ret == ESP_OK) {
        psa_hash_finish(&hash, sha256_out, 32, &hash_len);
    } else {
        psa_hash_abort(&hash);
    }
    heap_caps_free(buf);
    return ret;
}

int sc_ota::download_from(esp_ota_handle_t handle, size_t offset, psa_hash_operation_t *hash, uint8_t *buf)
{
    esp_http_client_config_t cfg = {};
    cfg.url = params.url;
    cfg.timeout_ms = HTTP_TIMEOUT_MS;
    cfg.buffer_size = CONFIG_SOULCLOUD_OTA_HTTP_BUFFER;
    cfg.crt_bundle_attach = esp_crt_bundle_attach;
    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    if (client == nullptr) {
        return -1;
    }

    char range[32] = {};
    if (offset > 0) {
        snprintf(range, sizeof(range), "bytes=%u-", offset);
        esp_http_client_set_header(client, "Range", range);
    }

    int total_read = 0;
    int status = 0;
    if (esp_http_client_open(client, 0) == ESP_OK && esp_http_client_fetch_headers(client) >= 0) {
        status = esp_http_client_get_status_code(client);
    }

    // A resumed download must get 206; 200 would restart at byte 0 and corrupt the image.
    bool status_ok = offset == 0 ? status == 200 : status == 206;
    if (!status_ok) {
        ESP_LOGE(TAG, "download: HTTP status %d at offset %u", status, offset);
        total_read = status >= 400 ? -2 : -1; // 4xx/5xx (e.g. expired link) are not retried
    }

    while (status_ok && offset + total_read < params.size && !cancel_requested.load()) {
        size_t want = params.size - offset - total_read;
        if (want > CONFIG_SOULCLOUD_OTA_HTTP_BUFFER) {
            want = CONFIG_SOULCLOUD_OTA_HTTP_BUFFER;
        }
        int read_len = esp_http_client_read(client, reinterpret_cast<char *>(buf), static_cast<int>(want));
        if (read_len <= 0) {
            break;
        }

        auto ret = esp_ota_write(handle, buf, read_len);
        if (ret != ESP_OK) {
            ESP_LOGE(TAG, "download: flash write failed: 0x%x %s", ret, esp_err_to_name(ret));
            total_read = -2;
            break;
        }
        psa_hash_update(hash, buf, read_len);
        total_read += read_len;

        int progress = static_cast<int>((static_cast<uint64_t>(offset + total_read) * 100) / params.size);
        int64_t now = esp_timer_get_time();
        if (progress != last_progress && now - last_report_us >= REPORT_INTERVAL_US) {
            report("downloading", progress, nullptr);
            last_progress = progress;
            last_report_us = now;
        }
    }

    esp_http_client_close(client);
    esp_http_client_cleanup(client);
    return total_read == 0 ? -1 : total_read;
}
