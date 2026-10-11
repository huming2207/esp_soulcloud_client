#pragma once

#include <esp_err.h>

#include "soulcloud.hpp"

class sc_mqtt;

/**
 * Status reports on d/<id>/up/status: a birth message after each connect,
 * then a heartbeat with uptime, reset reason, running firmware and heap.
 * Applications add fields (RSSI, sensor data) through the status callback.
 *
 * Runs on the request task only.
 */
class sc_status
{
public:
    esp_err_t init(sc_mqtt *_mqtt, uint32_t _boot_id);
    void set_extra_cb(const sc_status_cb &cb);
    esp_err_t report(bool birth);

private:
    sc_mqtt *mqtt = nullptr;
    uint32_t boot_id = 0;
    sc_status_cb extra_cb = {};
    char buf[640] = {};

    static const constexpr char *TAG = "sc_status";
};
