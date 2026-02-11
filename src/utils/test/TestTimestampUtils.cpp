#include <cassert>
#include <iostream>
#include <variant>
#include <string>
#include <thread>
#include <chrono>
#include <stdexcept>
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
    // 2023-01-01 00:00:00
    std::tm tm = {};
    tm.tm_year = 2023 - 1900;
    tm.tm_mon = 0;
    tm.tm_mday = 1;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    tm.tm_isdst = -1;
    time_t expected = std::mktime(&tm);
    int64_t result = TimestampUtils::parse_timestamp("2023-01-01 00:00:00", "s");
    (void)expected;
    (void)result;
    assert(result == static_cast<int64_t>(expected));
    std::cout << "test_parse_timestamp_iso_string passed\n";
}

void test_precision_multiplier() {
    assert(TimestampUtils::get_precision_multiplier("s") == 1);
    assert(TimestampUtils::get_precision_multiplier("ms") == 1000);
    assert(TimestampUtils::get_precision_multiplier("us") == 1000000LL);
    assert(TimestampUtils::get_precision_multiplier("ns") == 1000000000LL);

    bool threw = false;
    try {
        TimestampUtils::get_precision_multiplier("invalid");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    (void)threw;
    assert(threw);
    std::cout << "test_precision_multiplier passed\n";
}

void test_precision_conversion() {
    int64_t ts = 1000;
    (void)ts;
    assert(TimestampUtils::convert_timestamp_precision(ts, "ms", "ms") == ts);
    assert(TimestampUtils::convert_timestamp_precision(ts, "ms", "us") == 1000000);
    assert(TimestampUtils::convert_timestamp_precision(ts, "ms", "ns") == 1000000000);
    assert(TimestampUtils::convert_timestamp_precision(ts, "s", "ms") == 1000000);

    double d = TimestampUtils::convert_timestamp_precision_double(1, "s", "ms");
    (void)d;
    assert(d == 1000.0);
    std::cout << "test_precision_conversion passed\n";
}

void test_parse_timestamp_iso_utc_z() {
    int64_t result = TimestampUtils::parse_timestamp("2023-01-01T00:00:00Z", "s");
#if defined(_WIN32)
    std::tm tm = {};
    tm.tm_year = 2023 - 1900;
    tm.tm_mon = 0;
    tm.tm_mday = 1;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    tm.tm_isdst = -1;
    time_t expected = _mkgmtime(&tm);
    assert(result == static_cast<int64_t>(expected));
#else
    (void)result;
#endif
    std::cout << "test_parse_timestamp_iso_utc_z passed\n";
}

void test_parse_timestamp_invalid_inputs() {
    bool threw = false;
    try {
        TimestampUtils::parse_timestamp("not-a-time", "ms");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    (void)threw;
    assert(threw);

    threw = false;
    try {
        TimestampUtils::parse_timestamp("now()+abc", "ms");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    threw = false;
    try {
        TimestampUtils::parse_timestamp("now()+10x", "ms");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    threw = false;
    try {
        TimestampUtils::parse_timestamp("now()+10", "bad");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    threw = false;
    try {
        TimestampUtils::parse_timestamp(std::string("999999999999999999999999"), "ms");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    std::cout << "test_parse_timestamp_invalid_inputs passed\n";
}

void test_parse_step_basic() {
    assert(TimestampUtils::parse_step(10LL, "ms") == 10);
    assert(TimestampUtils::parse_step(std::string("10ms"), "ms") == 10);
    assert(TimestampUtils::parse_step(std::string("1000us"), "ms") == 1);
    assert(TimestampUtils::parse_step(std::string("2s"), "ms") == 2000);
    assert(TimestampUtils::parse_step(std::string("5"), "ms") == 5);

    std::cout << "test_parse_step_basic passed\n";
}

void test_parse_step_invalid() {
    bool threw = false;
    try {
        TimestampUtils::parse_step(std::string("ms"), "ms");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    (void)threw;
    assert(threw);

    threw = false;
    try {
        TimestampUtils::parse_step(std::string("10x"), "ms");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    threw = false;
    try {
        TimestampUtils::parse_step(std::string("10"), "bad");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);

    std::cout << "test_parse_step_invalid passed\n";
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
    test_precision_multiplier();
    test_precision_conversion();
    test_parse_timestamp_iso_utc_z();
    test_parse_timestamp_invalid_inputs();
    test_parse_step_basic();
    test_parse_step_invalid();

    std::cout << "All TimestampUtils tests passed!\n";
    return 0;
}