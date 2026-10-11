#pragma once

#include <cstddef>
#include <cstdint>

#include <esp_err.h>

/**
 * Compact JSON written into a caller-owned buffer; never allocates.
 *
 * Keys and strings are escaped. A @p key of nullptr adds an array element.
 * Once the buffer is full every further call is ignored and ok() returns
 * false: the text is then incomplete and must not be sent. The output is
 * always NUL-terminated.
 */
class sc_json_writer
{
public:
    sc_json_writer(char *_buf, size_t _cap);

    sc_json_writer &begin_object(const char *key = nullptr);
    sc_json_writer &end_object();
    sc_json_writer &begin_array(const char *key = nullptr);
    sc_json_writer &end_array();

    /** A string value; nullptr writes null. */
    sc_json_writer &add_str(const char *key, const char *value);
    sc_json_writer &add_str(const char *key, const char *value, size_t len);
    /** A string value formatted with printf. */
    sc_json_writer &add_fmt(const char *key, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
    sc_json_writer &add_int(const char *key, int64_t value);
    sc_json_writer &add_uint(const char *key, uint64_t value);
    sc_json_writer &add_bool(const char *key, bool value);
    sc_json_writer &add_null(const char *key);
    /** Lower-case hex string of @p len bytes. */
    sc_json_writer &add_hex(const char *key, const uint8_t *bytes, size_t len);

    void reset();

    bool ok() const
    {
        return !overflow;
    }

    size_t len() const
    {
        return pos;
    }

    const char *c_str() const
    {
        return buf;
    }

private:
    void begin_value(const char *key);
    void put(char c);
    void put(const char *str, size_t len);
    void put_escaped(const char *str, size_t len);

    char *buf = nullptr;
    size_t cap = 0;
    size_t pos = 0;
    bool overflow = false;
    uint8_t depth = 0;
    uint32_t has_items = 0; // Bit n: the container at depth n already holds a value

    static const constexpr uint8_t MAX_DEPTH = 31;
};

/** One token of a parsed document. Same layout as jsmntok_t. */
struct sc_json_token {
    int type;
    int start;
    int end;
    int size;
};

/**
 * Reads a JSON object in place; never allocates.
 *
 * parse() tokenises the text into the caller's token array, and the lookup
 * helpers return token indices into it. Index 0 is the root object. The
 * text and the tokens must stay valid while the reader is used.
 */
class sc_json_reader
{
public:
    sc_json_reader(sc_json_token *_tokens, size_t _max_tokens);

    /**
     * ESP_ERR_INVALID_ARG for malformed JSON or a root that is not an object,
     * ESP_ERR_NO_MEM when the document needs more tokens than provided.
     */
    esp_err_t parse(const char *_json, size_t _len);

    /** Value token of @p key in the object at @p object, or -1. */
    int find(int object, const char *key) const;

    /** Token after @p tok's whole subtree, or -1 past the end. */
    int skip(int tok) const;

    bool is_object(int tok) const;

    /** Number of members (objects) or elements (arrays) of @p tok. */
    int child_count(int tok) const;

    /** Raw text of @p tok (strings without quotes, still escaped). */
    const char *raw(int tok, size_t *len) const;

    /** Unescaped string value. False when missing, not a string or longer than @p cap - 1. */
    bool get_str(int object, const char *key, char *out, size_t cap) const;
    bool str_at(int tok, char *out, size_t cap) const;
    bool get_int(int object, const char *key, int64_t &out) const;
    bool int_at(int tok, int64_t &out) const;
    bool get_bool(int object, const char *key, bool &out) const;

private:
    bool key_equals(int tok, const char *key) const;

    sc_json_token *tokens = nullptr;
    size_t max_tokens = 0;
    int count = 0;
    const char *json = nullptr;
    size_t len = 0;
};

/** sc_json_reader with its token storage, for use on the stack. */
template <size_t N> class sc_json_doc : public sc_json_reader
{
public:
    sc_json_doc() : sc_json_reader(storage, N) {}

private:
    sc_json_token storage[N] = {};
};
