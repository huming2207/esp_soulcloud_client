#pragma once

#include <esp_err.h>

#include "sc_messages.hpp"

/**
 * The device identity issued by the platform at enrollment: device ID (also
 * the MQTT username and client ID), MQTT password and broker URL. Stored in
 * NVS namespace "soulcloud"; every enrollment replaces all three.
 */
class sc_credentials
{
public:
    /** Read the stored identity. ESP_ERR_NOT_FOUND when the device is not enrolled. */
    esp_err_t load();

    /**
     * Redeem @p token at @p api_url (POST /v1/device/enroll), then store and
     * use the new identity. Needs the network; blocks for the HTTP exchange.
     */
    esp_err_t enroll(const char *api_url, const char *token, const char *name);

    esp_err_t erase();

    bool valid() const
    {
        return identity.device_id[0] != '\0';
    }

    const char *device_id() const
    {
        return identity.device_id;
    }

    const char *password() const
    {
        return identity.password;
    }

    const char *mqtt_url() const
    {
        return identity.mqtt_url;
    }

private:
    esp_err_t save(const sc_enroll_response &resp);

    sc_enroll_response identity = {};

    static const constexpr size_t RESPONSE_MAX = 1024;
    static const constexpr int HTTP_TIMEOUT_MS = 15000;
    static const constexpr char NVS_NAMESPACE[] = "soulcloud";
    static const constexpr char KEY_DEVICE_ID[] = "dev_id";
    static const constexpr char KEY_PASSWORD[] = "password";
    static const constexpr char KEY_MQTT_URL[] = "mqtt_url";
    static const constexpr char *TAG = "sc_creds";
};
