// Host test for sc_json: g++ -std=c++20 -Istubs -I../../include -I../../external/jsmn json_test.cpp ../../src/sc_json.cpp
#include <cstdio>
#include <cstring>

#include "sc_json.hpp"

static int failures = 0;

#define CHECK(cond)                                                                                                                                  \
    do {                                                                                                                                             \
        if (!(cond)) {                                                                                                                               \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                                                                                   \
            failures += 1;                                                                                                                           \
        }                                                                                                                                            \
    } while (0)

static void test_writer()
{
    char buf[256] = {};
    sc_json_writer out(buf, sizeof(buf));
    out.begin_object().add_bool("ok", true).begin_object("result").add_int("code", -1).add_str("output", "a\"b\\c\n\x01");
    out.begin_array("list").add_uint(nullptr, 1).add_uint(nullptr, 2).end_array();
    const uint8_t bytes[] = {0x00, 0xab};
    out.add_hex("hex", bytes, sizeof(bytes)).add_null("none").end_object().end_object();
    CHECK(out.ok());
    CHECK(
        strcmp(buf, "{\"ok\":true,\"result\":{\"code\":-1,\"output\":\"a\\\"b\\\\c\\n\\u0001\",\"list\":[1,2],\"hex\":\"00ab\",\"none\":null}}") == 0
    );

    char small[8] = {};
    sc_json_writer tiny(small, sizeof(small));
    tiny.begin_object().add_str("key", "value").end_object();
    CHECK(!tiny.ok());
    CHECK(strlen(small) < sizeof(small)); // Still NUL-terminated
}

static void test_reader()
{
    const char *json = "{\"a\":{\"x\":[1,{\"deep\":true}],\"y\":\"skip\"},\"url\":\"http:\\/\\/h\\u00e9\",\"n\":-42,\"f\":1.5,\"b\":false,"
                       "\"tags\":{\"wifi\":\"debug\",\"app\":\"warn\"}}";
    sc_json_doc<32> doc;
    CHECK(doc.parse(json, strlen(json)) == ESP_OK);

    char text[32] = {};
    CHECK(doc.get_str(0, "url", text, sizeof(text)));
    CHECK(strcmp(text, "http://h\xc3\xa9") == 0);

    int64_t n = 0;
    CHECK(doc.get_int(0, "n", n) && n == -42);
    CHECK(!doc.get_int(0, "f", n)); // Not a whole integer
    bool flag = true;
    CHECK(doc.get_bool(0, "b", flag) && !flag);

    // Nested lookups walk past the sibling subtree correctly.
    int a = doc.find(0, "a");
    CHECK(doc.is_object(a));
    CHECK(doc.get_str(a, "y", text, sizeof(text)) && strcmp(text, "skip") == 0);
    CHECK(doc.find(0, "missing") == -1);
    CHECK(doc.find(-1, "url") == -1);

    char tiny[4] = {};
    CHECK(!doc.get_str(0, "url", tiny, sizeof(tiny))); // Too long for the buffer

    int tags = doc.find(0, "tags");
    CHECK(doc.child_count(tags) == 2);

    sc_json_doc<4> small;
    CHECK(small.parse(json, strlen(json)) == ESP_ERR_NO_MEM);
    sc_json_doc<8> bad;
    CHECK(bad.parse("[1,2]", 5) == ESP_ERR_INVALID_ARG); // Root must be an object
    // jsmn accepts a key without a value; the reader treats it as missing.
    if (bad.parse("{\"a\":}", 6) == ESP_OK) {
        CHECK(bad.find(0, "a") == -1);
    }
    CHECK(bad.parse("{\"a\":1", 6) == ESP_ERR_INVALID_ARG); // Truncated
}

int main()
{
    test_writer();
    test_reader();
    printf(failures == 0 ? "all JSON tests passed\n" : "%d JSON test(s) failed\n", failures);
    return failures == 0 ? 0 : 1;
}
