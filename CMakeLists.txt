idf_component_register(
    SRCS
        "src/soulcloud.cpp"
        "src/config.cpp"
        "src/mqtt_bridge.cpp"
        "src/commands.cpp"
        "src/logs.cpp"
        "src/ota.cpp"
        "src/protocol.cpp"
        "external/mpack/src/mpack/mpack-common.c"
        "external/mpack/src/mpack/mpack-expect.c"
        "external/mpack/src/mpack/mpack-platform.c"
        "external/mpack/src/mpack/mpack-reader.c"
        "external/mpack/src/mpack/mpack-writer.c"
    INCLUDE_DIRS "include"
    PRIV_INCLUDE_DIRS "src" "external/mpack/src/mpack"
    REQUIRES on9log mqtt app_update esp_app_format esp_http_client esp_partition nvs_flash log esp_event esp_system esp_timer esp_ringbuf heap
    PRIV_REQUIRES mbedtls
)

# MPack includes mpack-config.h from mpack-platform.h when MPACK_HAS_CONFIG=1
target_compile_definitions(${COMPONENT_LIB} PRIVATE MPACK_HAS_CONFIG=1)
