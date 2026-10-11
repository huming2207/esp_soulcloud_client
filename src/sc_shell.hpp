#pragma once

#include <cstddef>
#include <cstdint>

#include <esp_err.h>

#include "soulcloud.hpp"

/**
 * The remote debug shell behind the "shell" method. Commands are looked up
 * by their leading words, longest name first, so "get free_heap" and
 * "get app_desc" can coexist. The generic commands from add_builtins() cover
 * uptime, firmware, memory, partitions, DNS and restart; applications add
 * their own through sc_client::register_shell_cmd().
 *
 * Runs on the request task only.
 */
class sc_shell
{
public:
    esp_err_t add(const char *name, const char *help, const sc_shell_handler &handler);
    void add_builtins();

    /** Run @p line (modified in place). Returns the exit code; 127 for an unknown command. */
    int run(char *line, sc_shell_out &out);

private:
    static const constexpr int MAX_ARGS = 16;

    struct entry {
        const char *name = nullptr;
        const char *help = nullptr;
        sc_shell_handler handler = {};
        uint8_t words = 0;
    };

    static uint8_t count_words(const char *name);
    static bool name_matches(const char *name, char **argv, int argc);

    entry cmds[CONFIG_SOULCLOUD_SHELL_MAX_CMDS] = {};

    static const constexpr char *TAG = "sc_shell";
};
