#include <cassert>
#include <iostream>
#include <variant>
#include <string>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <vector>
#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
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
    // 2023-01-01 00:00:00 in local time using cctz (thread-safe)
    static const cctz::time_zone local_tz = cctz::local_time_zone();
    cctz::civil_second cs(2023, 1, 1, 0, 0, 0);
    auto tp = cctz::convert(cs, local_tz);
    time_t expected = std::chrono::system_clock::to_time_t(tp);
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

// Performance optimization tests: fast path for numeric strings
void test_performance_numeric_fast_path() {
    // Test fast path with pure numeric strings (should be extremely fast)
    int64_t result1 = TimestampUtils::parse_timestamp("1700000000000", "ms");
    assert(result1 == 1700000000000);
    (void)result1;

    int64_t result2 = TimestampUtils::parse_timestamp("1609459200", "s");
    assert(result2 == 1609459200);
    (void)result2;

    // Test with leading/trailing spaces (should still work)
    int64_t result3 = TimestampUtils::parse_timestamp(std::string("  1700000000000  "), "ms");
    assert(result3 == 1700000000000);
    (void)result3;

    std::cout << "test_performance_numeric_fast_path passed\n";
}

// Performance optimization test: ISO UTC format with strptime
void test_performance_iso_utc_strptime() {
    // ISO format with T separator and Z suffix for UTC
    int64_t result1 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13Z", "s");
    assert(result1 > 0);  // Should be a valid positive timestamp
    (void)result1;

    // ISO format with space separator and Z suffix for UTC
    int64_t result2 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13Z", "ms");
    assert(result2 > 0);
    (void)result2;

    // Same UTC time should produce same result
    int64_t result3 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13Z", "ms");
    int64_t result4 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13Z", "ms");
    assert(result3 == result4);
    (void)result3; (void)result4;

    std::cout << "test_performance_iso_utc_strptime passed\n";
}

// Performance optimization test: ISO local time with cctz
void test_performance_iso_local_cctz() {
    // ISO format without Z suffix (local time, uses cctz)
    int64_t result1 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13", "s");
    assert(result1 > 0);  // Should be a valid positive timestamp
    (void)result1;

    // ISO format with space and no Z (local time)
    int64_t result2 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13", "ms");
    assert(result2 > 0);
    (void)result2;

    // Different formats for same local time should produce same result
    int64_t result3 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13", "ms");
    int64_t result4 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13", "ms");
    assert(result3 == result4);
    (void)result3; (void)result4;

    std::cout << "test_performance_iso_local_cctz passed\n";
}

// Thread safety test: multiple threads parsing timestamps concurrently
void test_thread_safety_concurrent_parsing() {
    static constexpr int NUM_THREADS = 10;
    static constexpr int ITERATIONS_PER_THREAD = 100;
    std::vector<std::thread> threads;
    std::vector<bool> results(NUM_THREADS, true);

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([t, &results]() {
            try {
                for (int i = 0; i < ITERATIONS_PER_THREAD; ++i) {
                    // Mix of different timestamp formats
                    int64_t r1 = TimestampUtils::parse_timestamp("1700000000000", "ms");
                    int64_t r2 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13Z", "s");
                    int64_t r3 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13", "ms");
                    int64_t r4 = TimestampUtils::parse_timestamp("now()+10s", "s");

                    (void)r1; (void)r2; (void)r3; (void)r4;
                }
            } catch (...) {
                results[t] = false;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (int t = 0; t < NUM_THREADS; ++t) {
        assert(results[t]);
    }

    std::cout << "test_thread_safety_concurrent_parsing passed\n";
}

// Test precision conversion with various formats
void test_precision_conversion_mixed_formats() {
    // Numeric to different precisions
    int64_t ms_val = 1700000000000;
    int64_t us_val = TimestampUtils::convert_timestamp_precision(ms_val, "ms", "us");
    assert(us_val == ms_val * 1000);
    (void)us_val;

    int64_t s_val = TimestampUtils::convert_timestamp_precision(ms_val, "ms", "s");
    assert(s_val == ms_val / 1000);

    // Verify round-trip conversion
    int64_t back_to_ms = TimestampUtils::convert_timestamp_precision(s_val, "s", "ms");
    assert(back_to_ms == ms_val);
    (void)back_to_ms;

    std::cout << "test_precision_conversion_mixed_formats passed\n";
}

// Test ISO UTC format with fractional seconds
void test_parse_timestamp_iso_utc_fractional() {
    // UTC format with fractional seconds
    int64_t result1 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.123Z", "ms");
    assert(result1 > 0);
    (void)result1;

    // UTC format with space and fractional seconds
    int64_t result2 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.456Z", "ms");
    assert(result2 > 0);
    (void)result2;

    // Same UTC time with different fractional seconds should differ by the fractional part
    int64_t result3 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.000Z", "ms");
    int64_t result4 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.100Z", "ms");
    assert(result4 - result3 == 100);  // 100ms difference
    (void)result3; (void)result4;

    std::cout << "test_parse_timestamp_iso_utc_fractional passed\n";
}

// Test ISO local format with fractional seconds
void test_parse_timestamp_iso_local_fractional() {
    // Local format with fractional seconds
    int64_t result1 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.789", "ms");
    assert(result1 > 0);
    (void)result1;

    // Local format with space and fractional seconds
    int64_t result2 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.321", "ms");
    assert(result2 > 0);
    (void)result2;

    // Same local time with different fractional seconds should differ
    int64_t result3 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.000", "ms");
    int64_t result4 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.500", "ms");
    assert(result4 - result3 == 500);  // 500ms difference
    (void)result3; (void)result4;

    std::cout << "test_parse_timestamp_iso_local_fractional passed\n";
}

// Test fractional seconds precision conversion
void test_parse_timestamp_fractional_precision_conversion() {
    // Parse with millisecond precision and different fractional parts
    int64_t ms_result = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.738", "ms");
    assert(ms_result > 0);
    (void)ms_result;

    // Parse same timestamp in seconds precision
    int64_t s_result = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.738", "s");
    assert(s_result > 0);
    (void)s_result;

    // Parse same timestamp in microseconds precision
    int64_t us_result = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.738", "us");
    assert(us_result > 0);
    (void)us_result;

    // Verify relationship: ms_result * 1000 should equal us_result (approximately)
    assert(ms_result * 1000 == us_result);

    std::cout << "test_parse_timestamp_fractional_precision_conversion passed\n";
}

// Test very small fractional seconds (microseconds within milliseconds)
void test_parse_timestamp_fractional_small() {
    // Test parsing with single digit fractional seconds
    int64_t result1 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.1", "ms");
    assert(result1 > 0);
    (void)result1;

    // Test parsing with two digit fractional seconds
    int64_t result2 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.12", "ms");
    assert(result2 > 0);
    (void)result2;

    // Test parsing with full three digit fractional seconds
    int64_t result3 = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.123", "ms");
    assert(result3 > 0);
    (void)result3;

    // Verify padding behavior: .1 should be treated as .100 (100ms)
    int64_t result_01 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.1Z", "ms");
    int64_t result_100 = TimestampUtils::parse_timestamp("2023-03-25T15:47:13.100Z", "ms");
    (void)result_01; (void)result_100;
    assert(result_01 == result_100);

    std::cout << "test_parse_timestamp_fractional_small passed\n";
}

// Test the exact error case from CSV streaming: 2026-03-25 16:22:45.738
void test_parse_timestamp_csv_streaming_case() {
    try {
        // This is the exact timestamp that was failing in CSV streaming
        int64_t result = TimestampUtils::parse_timestamp("2026-03-25 16:22:45.738", "ms");
        assert(result > 0);
        (void)result;

        // Also test with UTC Z suffix
        int64_t result_utc = TimestampUtils::parse_timestamp("2026-03-25T16:22:45.738Z", "ms");
        assert(result_utc > 0);
        (void)result_utc;

        // Verify fractional milliseconds are preserved
        int64_t result_base = TimestampUtils::parse_timestamp("2026-03-25 16:22:45.000", "ms");
        int64_t diff = result - result_base;
        (void)diff;
        assert(diff == 738);  // .738 should add exactly 738 milliseconds

        std::cout << "test_parse_timestamp_csv_streaming_case passed\n";
    } catch (const std::exception& e) {
        // Rethrow to show the original issue is fixed
        throw std::runtime_error(std::string("CSV streaming timestamp case failed: ") + e.what());
    }
}

// Test microsecond precision (6 decimal digits)
void test_parse_timestamp_microsecond_precision() {
    // Test parsing with microseconds
    int64_t result_us = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.123456", "us");
    assert(result_us > 0);
    (void)result_us;

    // Same timestamp in milliseconds should be 123 (rounded down from 123.456)
    int64_t result_ms = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.123456", "ms");
    assert(result_ms > 0);
    (void)result_ms;

    // Verify microsecond difference: .123456 = 123456 microseconds
    int64_t result_base = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.000000", "us");
    int64_t diff_us = result_us - result_base;
    (void)diff_us;
    assert(diff_us == 123456);

    std::cout << "test_parse_timestamp_microsecond_precision passed\n";
}

// Test nanosecond precision (9 decimal digits)
void test_parse_timestamp_nanosecond_precision() {
    // Test parsing with nanoseconds
    int64_t result_ns = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.123456789", "ns");
    assert(result_ns > 0);
    (void)result_ns;

    // Verify nanosecond difference: .123456789 = 123456789 nanoseconds
    int64_t result_base = TimestampUtils::parse_timestamp("2023-03-25 15:47:13.000000000", "ns");
    int64_t diff_ns = result_ns - result_base;
    (void)diff_ns;
    assert(diff_ns == 123456789);

    std::cout << "test_parse_timestamp_nanosecond_precision passed\n";
}

// Test precision conversion between different scales
void test_parse_timestamp_precision_scales() {
    std::string timestamp = "2023-03-25 15:47:13.123456789";

    // Parse in different precisions
    int64_t s_val = TimestampUtils::parse_timestamp(timestamp, "s");
    int64_t ms_val = TimestampUtils::parse_timestamp(timestamp, "ms");
    int64_t us_val = TimestampUtils::parse_timestamp(timestamp, "us");
    int64_t ns_val = TimestampUtils::parse_timestamp(timestamp, "ns");
    (void)s_val; (void)ms_val; (void)us_val; (void)ns_val;

    // Verify relationships (accounting for fractional truncation at each scale)
    // s_val truncates to whole seconds, so ms_val != s_val * 1000 when fractional part exists
    // Instead, verify that lower precisions are consistent with higher ones
    assert(ms_val / 1000 == s_val);       // truncating ms to s matches s_val
    assert(us_val / 1000 == ms_val);      // truncating us to ms matches ms_val
    assert(ns_val / 1000 == us_val);      // truncating ns to us matches us_val

    std::cout << "test_parse_timestamp_precision_scales passed\n";
}

// Test UTC format with high precision
void test_parse_timestamp_utc_nanosecond() {
    int64_t result = TimestampUtils::parse_timestamp("2026-03-25T16:22:45.987654321Z", "ns");
    assert(result > 0);
    (void)result;

    int64_t result_base = TimestampUtils::parse_timestamp("2026-03-25T16:22:45.000000000Z", "ns");
    int64_t diff = result - result_base;
    (void)diff;
    assert(diff == 987654321);

    std::cout << "test_parse_timestamp_utc_nanosecond passed\n";
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

    // Performance optimization tests
    test_performance_numeric_fast_path();
    test_performance_iso_utc_strptime();
    test_performance_iso_local_cctz();
    test_thread_safety_concurrent_parsing();
    test_precision_conversion_mixed_formats();

    // Fractional seconds tests
    test_parse_timestamp_iso_utc_fractional();
    test_parse_timestamp_iso_local_fractional();
    test_parse_timestamp_fractional_precision_conversion();
    test_parse_timestamp_fractional_small();
    test_parse_timestamp_csv_streaming_case();

    // High precision tests (microseconds and nanoseconds)
    test_parse_timestamp_microsecond_precision();
    test_parse_timestamp_nanosecond_precision();
    test_parse_timestamp_precision_scales();
    test_parse_timestamp_utc_nanosecond();

    std::cout << "All TimestampUtils tests passed!\n";
    return 0;
}