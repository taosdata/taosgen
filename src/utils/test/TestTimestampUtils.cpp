#include <cassert>
#include <iostream>
#include <variant>
#include <string>
#include <thread>
#include <chrono>
#include "TimestampUtils.hpp"

// Test: pass int64_t directly
void test_parse_timestamp_int64() {
    int64_t ts = 1700000000000;
    int64_t result = TimestampUtils::parse_timestamp(ts, "ms");
    (void)result;
    assert(result == ts);
    std::cout << "test_parse_timestamp_int64 passed\n";
}

// Test: string integer
void test_parse_timestamp_string_int() {
    std::string ts = "1700000000000";
    int64_t result = TimestampUtils::parse_timestamp(ts, "ms");
    (void)result;
    assert(result == 1700000000000);
    std::cout << "test_parse_timestamp_string_int passed\n";
}

// Test: now() with precision
void test_parse_timestamp_now() {
    int64_t before = TimestampUtils::convert_to_timestamp("ms");
    int64_t result = TimestampUtils::parse_timestamp("now()", "ms");
    int64_t after = TimestampUtils::convert_to_timestamp("ms");
    (void)before;
    (void)result;
    (void)after;
    assert(result >= before && result <= after);
    std::cout << "test_parse_timestamp_now passed\n";
}

// Test: now()+10s
void test_parse_timestamp_now_plus_10s() {
    int64_t before = TimestampUtils::convert_to_timestamp("s");
    int64_t result = TimestampUtils::parse_timestamp("now()+10s", "s");
    int64_t after = TimestampUtils::convert_to_timestamp("s");
    (void)before;
    (void)result;
    (void)after;
    assert(result >= before + 10 && result <= after + 10);
    std::cout << "test_parse_timestamp_now_plus_10s passed\n";
}

// Test: now()-5ms
void test_parse_timestamp_now_minus_5ms() {
    int64_t before = TimestampUtils::convert_to_timestamp("ms");
    int64_t result = TimestampUtils::parse_timestamp("now()-5ms", "ms");
    int64_t after = TimestampUtils::convert_to_timestamp("ms");
    (void)before;
    (void)result;
    (void)after;
    assert(result <= after - 5 && result >= before - 5);
    std::cout << "test_parse_timestamp_now_minus_5ms passed\n";
}

// Test: now()+2h
void test_parse_timestamp_now_plus_2h() {
    int64_t before = TimestampUtils::convert_to_timestamp("s");
    int64_t result = TimestampUtils::parse_timestamp("now()+2h", "s");
    int64_t after = TimestampUtils::convert_to_timestamp("s");
    (void)before;
    (void)result;
    (void)after;
    assert(result >= before + 7200 && result <= after + 7200);
    std::cout << "test_parse_timestamp_now_plus_2h passed\n";
}

// Test: now()+1d
void test_parse_timestamp_now_plus_1d() {
    int64_t before = TimestampUtils::convert_to_timestamp("s");
    int64_t result = TimestampUtils::parse_timestamp("now()+1d", "s");
    int64_t after = TimestampUtils::convert_to_timestamp("s");
    (void)before;
    (void)result;
    (void)after;
    assert(result >= before + 86400 && result <= after + 86400);
    std::cout << "test_parse_timestamp_now_plus_1d passed\n";
}

// Test: now()+100 (no unit, use precision)
void test_parse_timestamp_now_plus_100_default_precision() {
    int64_t before = TimestampUtils::convert_to_timestamp("ms");
    int64_t result = TimestampUtils::parse_timestamp("now()+100", "ms");
    int64_t after = TimestampUtils::convert_to_timestamp("ms");
    (void)before;
    (void)result;
    (void)after;
    assert(result >= before + 100 && result <= after + 100);
    std::cout << "test_parse_timestamp_now_plus_100_default_precision passed\n";
}

// Test: ISO time string
void test_parse_timestamp_iso_string() {
    // 2023-01-01 00:00:00 = 1672502400
    int64_t result = TimestampUtils::parse_timestamp("2023-01-01 00:00:00", "s");
    (void)result;
    assert(result == 1672502400);
    std::cout << "test_parse_timestamp_iso_string passed\n";
}

int main() {
    test_parse_timestamp_int64();
    test_parse_timestamp_string_int();
    test_parse_timestamp_now();
    test_parse_timestamp_now_plus_10s();
    test_parse_timestamp_now_minus_5ms();
    test_parse_timestamp_now_plus_2h();
    test_parse_timestamp_now_plus_1d();
    test_parse_timestamp_now_plus_100_default_precision();
    test_parse_timestamp_iso_string();

    std::cout << "All TimestampUtils tests passed!\n";
    return 0;
}