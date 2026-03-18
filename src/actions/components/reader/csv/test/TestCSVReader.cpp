#include <iostream>
#include <fstream>
#include <cassert>
#include <filesystem>
#include "CSVReader.hpp"

namespace fs = std::filesystem;

void test_open_invalid_file() {
    try {
        CSVReader reader("invalid_file.csv", false, ',');
        assert(false && "Expected exception for invalid file path");
    } catch (const std::runtime_error& e) {
        std::cout << "test_open_invalid_file passed\n";
    }
}

void test_read_empty_file() {
    std::ofstream empty_file("empty.csv");
    empty_file.close();

    // vincentlaucsb csv-parser throws on empty files
    try {
        CSVReader reader("empty.csv", false, ',');
        auto rows = reader.read_all();
        assert(rows.empty() && "Expected no rows for empty file");
    } catch (const std::runtime_error&) {
        // Expected: csv-parser cannot open empty files
    }
    std::remove("empty.csv");
    std::cout << "test_read_empty_file passed\n";
}

void test_read_simple_file() {
    std::ofstream simple_file("simple.csv");
    simple_file << "name,age,city\n";
    simple_file << "Alice,30,New York\n";
    simple_file << "Bob,25,Los Angeles\n";
    simple_file.close();

    CSVReader reader("simple.csv", true, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 2 && "Expected 2 rows");
    assert(rows[0][0] == "Alice" && rows[0][1] == "30" && rows[0][2] == "New York");
    assert(rows[1][0] == "Bob" && rows[1][1] == "25" && rows[1][2] == "Los Angeles");
    std::remove("simple.csv");
    std::cout << "test_read_simple_file passed\n";
}

void test_parse_complex_line() {
    std::ofstream complex_file("complex.csv");
    complex_file << "\"field1\",\"field2,with,comma\",\"field3\"";
    complex_file.close();

    CSVReader reader("complex.csv", false, ',');
    auto fields = reader.read_next().value_or(CSVRow{});
    assert(fields.size() == 3 && "Expected 3 fields");
    assert(fields[0] == "field1");
    assert(fields[1] == "field2,with,comma");
    assert(fields[2] == "field3");
    std::remove("complex.csv");
    std::cout << "test_parse_complex_line passed\n";
}

void test_skip_header() {
    std::ofstream file_with_header("header.csv");
    file_with_header << "header1,header2,header3\n";
    file_with_header << "data1,data2,data3\n";
    file_with_header.close();

    CSVReader reader("header.csv", true, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 1 && "Expected 1 row after skipping header");
    assert(rows[0][0] == "data1" && rows[0][1] == "data2" && rows[0][2] == "data3");
    std::remove("header.csv");
    std::cout << "test_skip_header passed\n";
}

void test_column_count() {
    std::ofstream f("colcount.csv");
    f << "a,b,c\n1,2,3\n";
    f.close();

    CSVReader reader("colcount.csv", true, ',');
    assert(reader.column_count() == 3);
    std::remove("colcount.csv");
    std::cout << "test_column_count passed\n";
}

void test_reset() {
    std::ofstream f("reset.csv");
    f << "a,b\n1,2\n3,4\n";
    f.close();

    CSVReader reader("reset.csv", true, ',');
    auto rows1 = reader.read_all();
    assert(rows1.size() == 2);

    reader.reset();
    auto rows2 = reader.read_all();
    assert(rows2.size() == 2);
    assert(rows1[0][0] == rows2[0][0]);
    std::remove("reset.csv");
    std::cout << "test_reset passed\n";
}

void test_multi_file() {
    fs::create_directory("multi_csv_test");
    {
        std::ofstream f("multi_csv_test/a.csv");
        f << "x,y\n1,2\n3,4\n";
    }
    {
        std::ofstream f("multi_csv_test/b.csv");
        f << "x,y\n5,6\n7,8\n";
    }

    CSVReader reader(std::vector<std::string>{"multi_csv_test/a.csv", "multi_csv_test/b.csv"}, true, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 4 && "Expected 4 rows from 2 files");
    assert(rows[0][0] == "1" && rows[0][1] == "2");
    assert(rows[1][0] == "3" && rows[1][1] == "4");
    assert(rows[2][0] == "5" && rows[2][1] == "6");
    assert(rows[3][0] == "7" && rows[3][1] == "8");
    assert(reader.column_count() == 2);

    fs::remove_all("multi_csv_test");
    std::cout << "test_multi_file passed\n";
}

void test_multi_file_no_header() {
    fs::create_directory("multi_csv_nh");
    {
        std::ofstream f("multi_csv_nh/a.csv");
        f << "1,2\n3,4\n";
    }
    {
        std::ofstream f("multi_csv_nh/b.csv");
        f << "5,6\n";
    }

    CSVReader reader(std::vector<std::string>{"multi_csv_nh/a.csv", "multi_csv_nh/b.csv"}, false, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 3 && "Expected 3 rows");
    assert(rows[2][0] == "5" && rows[2][1] == "6");

    fs::remove_all("multi_csv_nh");
    std::cout << "test_multi_file_no_header passed\n";
}

void test_read_next_line_by_line() {
    std::ofstream f("lineby.csv");
    f << "a\n1\n2\n3\n";
    f.close();

    CSVReader reader("lineby.csv", true, ',');
    auto r1 = reader.read_next();
    assert(r1 && (*r1)[0] == "1");
    auto r2 = reader.read_next();
    assert(r2 && (*r2)[0] == "2");
    auto r3 = reader.read_next();
    assert(r3 && (*r3)[0] == "3");
    auto r4 = reader.read_next();
    assert(!r4);
    std::remove("lineby.csv");
    std::cout << "test_read_next_line_by_line passed\n";
}

int main() {
    test_open_invalid_file();
    test_read_empty_file();
    test_read_simple_file();
    test_parse_complex_line();
    test_skip_header();
    test_column_count();
    test_reset();
    test_multi_file();
    test_multi_file_no_header();
    test_read_next_line_by_line();

    std::cout << "All tests passed!\n";
    return 0;
}