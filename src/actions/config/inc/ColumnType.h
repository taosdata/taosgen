#pragma once

#include <iostream>
#include <variant>
#include <string>
#include <vector>
#include <cstdint>


using Timestamp = int64_t;

struct Decimal {
    std::string value;
};


// JSON type (can be replaced with nlohmann::json)
struct JsonValue {
    std::string raw_json;
};


// Geometry type (example: WKT format)
struct Geometry {
    std::string wkt; // e.g. "POINT(10 20)"
};

enum class ColumnTypeTag {
    UNKNOWN,
    BOOL,
    TINYINT,
    TINYINT_UNSIGNED,
    SMALLINT,
    SMALLINT_UNSIGNED,
    INT,
    INT_UNSIGNED,
    BIGINT,
    BIGINT_UNSIGNED,
    FLOAT,
    DOUBLE,
    DECIMAL,
    NCHAR,       // std::u16string
    VARCHAR,     // std::string (varchar/binary)
    BINARY,      // std::string (varchar/binary)
    JSON,        // std::string (json)
    VARBINARY,   // std::vector<uint8_t>
    GEOMETRY,    // Geometry
    MAX
};

using ColumnType = std::variant<
    bool,                 // bool
    int8_t,               // tinyint
    uint8_t,              // tinyint unsigned
    int16_t,              // smallint
    uint16_t,             // smallint unsigned
    int32_t,              // int
    uint32_t,             // int unsigned
    int64_t,              // bigint
    uint64_t,             // bigint unsigned
    float,                // float
    double,               // double
    Decimal,              // decimal
    std::u16string,       // nchar (UTF-16)
    std::string,          // varchar / binary
    JsonValue,            // json
    std::vector<uint8_t>, // varbinary
    Geometry              // geometry
>;

using ColumnTypeVector = std::vector<ColumnType>;
using RowType = ColumnTypeVector;


std::ostream& operator<<(std::ostream& os, const ColumnType& column);
std::ostream& operator<<(std::ostream& os, const RowType& row);