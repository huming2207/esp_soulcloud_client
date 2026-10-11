#pragma once

#include <cstddef>
#include <cstdint>

#include <esp_err.h>

/*
 * Messages exchanged with the platform, independent of their encoding. The
 * codec (sc_codec) turns them into wire payloads and back; features only see
 * these structs. Sizes match the platform's limits.
 */

/** d/<id>/up/status: birth after connecting, then the periodic heartbeat. */
struct sc_status_msg {
    bool birth = false;
    uint64_t uptime_ms = 0;
    uint32_t boot_id = 0;
    int reset_reason = 0; // esp_reset_reason_t
    const char *fw_version = nullptr;
    const char *project = nullptr;
    const char *sdk_version = nullptr;
    const uint8_t *elf_sha256 = nullptr; // 32 bytes
    const char *chip = nullptr;
    uint32_t heap_free = 0;
    uint32_t heap_min_free = 0;
};

/** ota.start parameters. */
struct sc_ota_params {
    char url[384] = {};
    char sha256[65] = {}; // Hex of the image's SHA-256
    uint32_t size = 0;
    char version[33] = {};
    char task_id[40] = {};
};

/** d/<id>/up/evt/ota: progress of an update. */
struct sc_ota_event {
    const char *task_id = nullptr;
    const char *state = nullptr; // downloading | verifying | rebooting | failed
    int progress = -1;           // 0-100, or -1 to leave out
    const char *error = nullptr;
};

/** log.config parameters. Levels are on9log_level_t; -1 = unchanged. */
struct sc_log_config {
    int level = -1;
    int8_t text = -1; // 1 enable, 0 disable, -1 unchanged
    int8_t bin = -1;
    // Per-tag levels are applied by the codec through a callback once all are valid.
};

/** Enrollment request sent to POST /v1/device/enroll. */
struct sc_enroll_request {
    const char *token = nullptr;
    const char *hardware_id = nullptr;
    const char *platform = nullptr;
    const char *chip = nullptr;
    const char *fw_version = nullptr;
    const char *name = nullptr;
};

/** Enrollment response. */
struct sc_enroll_response {
    char device_id[16] = {};
    char password[48] = {};
    char mqtt_url[160] = {};
};
