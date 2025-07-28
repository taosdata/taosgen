#include "ColumnConverter.h"
#include <cassert>
#include <string>
#include <vector>
#include <iostream>

void test_fixed_type_handler_int32() {
    ColumnType value = int32_t(12345);
    int32_t dest = 0;
    ColumnConverter::fixed_type_handler<int32_t>(value, &dest, sizeof(int32_t));
    assert(dest == 12345);
    std::cout << "test_fixed_type_handler_int32 passed." << std::endl;
}

void test_fixed_type_handler_float() {
    ColumnType value = 3.14f;
    float dest = 0.0f;
    ColumnConverter::fixed_type_handler<float>(value, &dest, sizeof(float));
    assert(dest == 3.14f);
    std::cout << "test_fixed_type_handler_float passed." << std::endl;
}

void test_fixed_type_handler_double() {
    ColumnType value = 3.14159265758;
    double dest = 0.0;
    ColumnConverter::fixed_type_handler<double>(value, &dest, sizeof(double));
    assert(dest == 3.14159265758);
    std::cout << "test_fixed_type_handler_double passed." << std::endl;
}

void test_string_type_handler() {
    ColumnType value = std::string("hello");
    char buf[16] = {};
    size_t len = ColumnConverter::string_type_handler(value, buf, sizeof(buf));
    (void)len;
    assert(len == 5);
    assert(std::string(buf, len) == "hello");
    std::cout << "test_string_type_handler passed." << std::endl;
}

void test_binary_type_handler() {
    ColumnType value = std::vector<uint8_t>{1, 2, 3, 4};
    char buf[8] = {};
    size_t len = ColumnConverter::binary_type_handler(value, buf, sizeof(buf));
    (void)len;
    assert(len == 4);
    assert(static_cast<uint8_t>(buf[0]) == 1);
    assert(static_cast<uint8_t>(buf[1]) == 2);
    assert(static_cast<uint8_t>(buf[2]) == 3);
    assert(static_cast<uint8_t>(buf[3]) == 4);
    std::cout << "test_binary_type_handler passed." << std::endl;
}

void test_create_handler_for_column() {
    ColumnConfig config{"col1", "INT"};
    ColumnConfigInstance instance(config);
    auto handler = ColumnConverter::create_handler_for_column(instance);
    (void)handler;
    assert(handler.fixed_handler != nullptr);
    assert(handler.var_handler == nullptr);
    std::cout << "test_create_handler_for_column passed." << std::endl;
}

void test_create_handlers_for_columns() {
    ColumnConfigInstanceVector vec;
    vec.emplace_back(ColumnConfig{"col1", "INT"});
    vec.emplace_back(ColumnConfig{"col2", "VARCHAR(20)"});
    auto handlers = ColumnConverter::create_handlers_for_columns(vec);
    assert(handlers.size() == 2);
    assert(handlers[0].fixed_handler != nullptr);
    assert(handlers[1].var_handler != nullptr);
    std::cout << "test_create_handlers_for_columns passed." << std::endl;
}

int main() {
    test_fixed_type_handler_int32();
    test_fixed_type_handler_float();
    test_fixed_type_handler_double();
    test_string_type_handler();
    test_binary_type_handler();
    test_create_handler_for_column();
    test_create_handlers_for_columns();

    std::cout << "All ColumnConverter tests passed." << std::endl;
    return 0;
}