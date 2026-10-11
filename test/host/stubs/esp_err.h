#pragma once
// Minimal stand-in for ESP-IDF's esp_err.h so sc_json builds on the host.
typedef int esp_err_t;
#define ESP_OK 0
#define ESP_ERR_NO_MEM 0x101
#define ESP_ERR_INVALID_ARG 0x102
