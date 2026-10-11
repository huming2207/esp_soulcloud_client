#include "sc_credentials.hpp"

#include <cstdio>
#include <cstring>

#include <esp_app_desc.h>
#include <esp_crt_bundle.h>
#include <esp_heap_caps.h>
#include <esp_http_client.h>
#include <esp_log.h>
#include <esp_mac.h>
#include <nvs.h>

#include "sc_codec.hpp"

esp_err_t sc_credentials::load()
{
    nvs_handle_t nvs = 0;
    auto ret = nvs_open(NVS_NAMESPACE, NVS_READONLY, &nvs);
    if (ret == ESP_ERR_NVS_NOT_FOUND) {
        return ESP_ERR_NOT_FOUND;
    }
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "load: can't open NVS: 0x%x %s", ret, esp_err_to_name(ret));
        return ret;
    }

    sc_enroll_response stored = {};
    size_t len = sizeof(stored.device_id);
    ret = nvs_get_str(nvs, KEY_DEVICE_ID, stored.device_id, &len);
    len = sizeof(stored.password);
    ret = ret ?: nvs_get_str(nvs, KEY_PASSWORD, stored.password, &len);
    len = sizeof(stored.mqtt_url);
    ret = ret ?: nvs_get_str(nvs, KEY_MQTT_URL, stored.mqtt_url, &len);
    nvs_close(nvs);

    if (ret == ESP_ERR_NVS_NOT_FOUND) {
        return ESP_ERR_NOT_FOUND;
    }
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "load: can't read credentials: 0x%x %s", ret, esp_err_to_name(ret));
        return ret;
    }

    identity = stored;
    return ESP_OK;
}

esp_err_t sc_credentials::save(const sc_enroll_response &resp)
{
    nvs_handle_t nvs = 0;
    auto ret = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &nvs);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "save: can't open NVS: 0x%x %s", ret, esp_err_to_name(ret));
        return ret;
    }

    ret = nvs_set_str(nvs, KEY_DEVICE_ID, resp.device_id);
    ret = ret ?: nvs_set_str(nvs, KEY_PASSWORD, resp.password);
    ret = ret ?: nvs_set_str(nvs, KEY_MQTT_URL, resp.mqtt_url);
    ret = ret ?: nvs_commit(nvs);
    nvs_close(nvs);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "save: can't write credentials: 0x%x %s", ret, esp_err_to_name(ret));
    }
    return ret;
}

esp_err_t sc_credentials::erase()
{
    nvs_handle_t nvs = 0;
    auto ret = nvs_open(NVS_NAMESPACE, NVS_READWRITE, &nvs);
    if (ret != ESP_OK) {
        return ret == ESP_ERR_NVS_NOT_FOUND ? ESP_OK : ret;
    }

    ret = nvs_erase_all(nvs);
    ret = ret ?: nvs_commit(nvs);
    nvs_close(nvs);
    identity = {};
    return ret;
}

esp_err_t sc_credentials::enroll(const char *api_url, const char *token, const char *name)
{
    if (api_url == nullptr || token == nullptr || token[0] == '\0') {
        return ESP_ERR_INVALID_ARG;
    }

    char url[192] = {};
    if (snprintf(url, sizeof(url), "%s/v1/device/enroll", api_url) >= static_cast<int>(sizeof(url))) {
        return ESP_ERR_INVALID_SIZE;
    }

    uint8_t mac[6] = {};
    char hardware_id[18] = {};
    esp_efuse_mac_get_default(mac);
    snprintf(hardware_id, sizeof(hardware_id), MACSTR, MAC2STR(mac));

    sc_enroll_request req = {};
    req.token = token;
    req.hardware_id = hardware_id;
    req.platform = "esp32";
    req.chip = CONFIG_IDF_TARGET;
    req.fw_version = esp_app_get_description()->version;
    req.name = name;

    char body[320] = {};
    size_t body_len = 0;
    auto ret = sc_codec::encode_enroll_request(req, body, sizeof(body), &body_len);
    if (ret != ESP_OK) {
        return ret;
    }

    // Enrollment happens once per device, so the response buffer is borrowed from the heap.
    auto *resp_buf = static_cast<char *>(heap_caps_malloc(RESPONSE_MAX, MALLOC_CAP_8BIT));
    if (resp_buf == nullptr) {
        return ESP_ERR_NO_MEM;
    }

    esp_http_client_config_t http_cfg = {};
    http_cfg.url = url;
    http_cfg.method = HTTP_METHOD_POST;
    http_cfg.timeout_ms = HTTP_TIMEOUT_MS;
    http_cfg.crt_bundle_attach = esp_crt_bundle_attach;
    esp_http_client_handle_t client = esp_http_client_init(&http_cfg);
    if (client == nullptr) {
        heap_caps_free(resp_buf);
        return ESP_ERR_NO_MEM;
    }

    int resp_len = 0;
    int status = 0;
    esp_http_client_set_header(client, "Content-Type", sc_codec::CONTENT_TYPE);
    ret = esp_http_client_open(client, static_cast<int>(body_len));
    if (ret == ESP_OK && esp_http_client_write(client, body, static_cast<int>(body_len)) != static_cast<int>(body_len)) {
        ret = ESP_FAIL;
    }
    if (ret == ESP_OK && esp_http_client_fetch_headers(client) < 0) {
        ret = ESP_FAIL;
    }
    if (ret == ESP_OK) {
        status = esp_http_client_get_status_code(client);
        while (resp_len < static_cast<int>(RESPONSE_MAX) - 1) {
            int read_len = esp_http_client_read(client, resp_buf + resp_len, RESPONSE_MAX - 1 - resp_len);
            if (read_len <= 0) {
                break;
            }
            resp_len += read_len;
        }
    }
    esp_http_client_close(client);
    esp_http_client_cleanup(client);

    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "enroll: request to %s failed: 0x%x %s", url, ret, esp_err_to_name(ret));
        heap_caps_free(resp_buf);
        return ret;
    }
    if (status != 201) {
        ESP_LOGE(TAG, "enroll: platform answered %d: %.*s", status, resp_len, resp_buf);
        heap_caps_free(resp_buf);
        return status == 401 ? ESP_ERR_INVALID_STATE : ESP_FAIL;
    }

    sc_enroll_response resp = {};
    ret = sc_codec::decode_enroll_response(resp_buf, resp_len, resp);
    heap_caps_free(resp_buf);
    ret = ret ?: save(resp);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "enroll: can't use the response: 0x%x %s", ret, esp_err_to_name(ret));
        return ret;
    }

    identity = resp;
    ESP_LOGI(TAG, "enroll: enrolled as %s", identity.device_id);
    return ESP_OK;
}
