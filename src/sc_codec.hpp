#pragma once

#include <cstddef>

#include <esp_err.h>

#include "sc_messages.hpp"
#include "soulcloud.hpp"

/**
 * Encodes and decodes the fixed platform messages. This build speaks JSON;
 * the structs in sc_messages.hpp are what the rest of the client uses, so a
 * second encoding only needs another implementation of this class.
 *
 * Encoders write into the caller's buffer and fail with ESP_ERR_NO_MEM when
 * it is too small. Decoders never allocate.
 */
class sc_codec
{
public:
    /** MQTT 5 Content-Type of every payload this codec produces. */
    static const constexpr char CONTENT_TYPE[] = "application/json";

    static esp_err_t encode_status(const sc_status_msg &msg, const sc_status_cb &extra, char *buf, size_t cap, size_t *out_len);
    static esp_err_t encode_ota_event(const sc_ota_event &evt, char *buf, size_t cap, size_t *out_len);
    static esp_err_t encode_enroll_request(const sc_enroll_request &req, char *buf, size_t cap, size_t *out_len);
    static esp_err_t decode_enroll_response(const char *payload, size_t len, sc_enroll_response &out);

    /** ota.start parameters, from a request payload or an ota.check result. */
    static esp_err_t decode_ota_params(const sc_json_reader &doc, int object, sc_ota_params &out);

    /** log.config: fills @p out and applies per-tag levels through @p set_tag. */
    static esp_err_t decode_log_config(const sc_json_reader &doc, sc_log_config &out, void (*set_tag)(const char *tag, int level));

    /** on9log level from its name (none, error, warn, info, debug, verbose), or -1. */
    static int level_from_name(const char *name);

private:
    static const constexpr char *TAG = "sc_codec";
};
