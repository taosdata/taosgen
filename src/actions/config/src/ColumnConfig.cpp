#include "ColumnConfig.h"
#include <stdexcept>
#include <cfloat>
#include <limits>
#include <regex>
#include <unordered_map>
#include "taos.h"
#include "StringUtils.h"


ColumnTypeTag ColumnConfig::get_type_tag(const std::string& type_str) {
    static const std::unordered_map<std::string, ColumnTypeTag> type_map = {
        {"bool", ColumnTypeTag::BOOL},
        {"tinyint", ColumnTypeTag::TINYINT},
        {"tinyint unsigned", ColumnTypeTag::TINYINT_UNSIGNED},
        {"smallint", ColumnTypeTag::SMALLINT},
        {"smallint unsigned", ColumnTypeTag::SMALLINT_UNSIGNED},
        {"int", ColumnTypeTag::INT},
        {"int unsigned", ColumnTypeTag::INT_UNSIGNED},
        {"bigint", ColumnTypeTag::BIGINT},
        {"bigint unsigned", ColumnTypeTag::BIGINT_UNSIGNED},
        {"float", ColumnTypeTag::FLOAT},
        {"double", ColumnTypeTag::DOUBLE},
        {"decimal", ColumnTypeTag::DECIMAL},
        {"nchar", ColumnTypeTag::NCHAR},
        {"varchar", ColumnTypeTag::VARCHAR},
        {"binary", ColumnTypeTag::BINARY},
        {"json", ColumnTypeTag::JSON},
        {"varbinary", ColumnTypeTag::VARBINARY},
        {"geometry", ColumnTypeTag::GEOMETRY}
    };

    std::string lower_type = StringUtils::to_lower(type_str);
    auto it = type_map.find(lower_type);
    if (it != type_map.end()) {
        return it->second;
    }
    throw std::runtime_error("Unsupported type: " + lower_type);
}

double ColumnConfig::get_min_value() const noexcept {
    static constexpr double min_values[] = {
        -1.0,                             // UNKNOWN
        0.0,                              // BOOL
        static_cast<double>(INT8_MIN),    // TINYINT
        0.0,                              // TINYINT_UNSIGNED
        static_cast<double>(INT16_MIN),   // SMALLINT
        0.0,                              // SMALLINT_UNSIGNED
        static_cast<double>(INT16_MIN),   // INT
        0.0,                              // INT_UNSIGNED
        static_cast<double>(INT16_MIN),   // BIGINT
        0.0,                              // BIGINT_UNSIGNED
        static_cast<double>(INT16_MIN),   // FLOAT
        static_cast<double>(INT16_MIN),   // DOUBLE
        static_cast<double>(INT16_MIN),   // DECIMAL
        -1.0,                             // NCHAR
        -1.0,                             // VARCHAR
        -1.0,                             // BINARY
        -1.0,                             // JSON
        -1.0,                             // VARBINARY
        -1.0                              // GEOMETRY
    };
    static_assert(sizeof(min_values)/sizeof(min_values[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "min_values size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(min_values)/sizeof(min_values[0])) {
        return min_values[idx];
    }
    return -1.0;
}

double ColumnConfig::get_max_value() const noexcept {
    static constexpr double max_values[] = {
        -1.0,                             // UNKNOWN
        1.0,                              // BOOL
        static_cast<double>(INT8_MAX),    // TINYINT
        static_cast<double>(UINT8_MAX),   // TINYINT_UNSIGNED
        static_cast<double>(INT16_MAX),   // SMALLINT
        static_cast<double>(UINT16_MAX),  // SMALLINT_UNSIGNED
        static_cast<double>(INT16_MAX),   // INT
        static_cast<double>(UINT16_MAX),  // INT_UNSIGNED
        static_cast<double>(INT16_MAX),   // BIGINT
        static_cast<double>(UINT16_MAX),  // BIGINT_UNSIGNED
        static_cast<double>(INT16_MAX),   // FLOAT
        static_cast<double>(INT16_MAX),   // DOUBLE
        static_cast<double>(INT16_MAX),   // DECIMAL
        -1.0,                             // NCHAR
        -1.0,                             // VARCHAR
        -1.0,                             // BINARY
        -1.0,                             // JSON
        -1.0,                             // VARBINARY
        -1.0                              // GEOMETRY
    };
    static_assert(sizeof(max_values)/sizeof(max_values[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "max_values size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(max_values)/sizeof(max_values[0])) {
        return max_values[idx];
    }
    return -1.0;
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type
) : name(name), type(type) {
    parse_type();
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::optional<std::string> gen_type
) : name(name), type(type) {
    parse_type();
    this->gen_type = gen_type;
}

ColumnConfig::ColumnConfig(
    const std::string& name,
    const std::string& type,
    std::optional<std::string> gen_type,
    std::optional<double> min,
    std::optional<double> max
) : name(name), type(type) {
    parse_type();
    this->gen_type = gen_type;
    this->min = min;
    this->max = max;
}

void ColumnConfig::parse_type() {
    StringUtils::trim(type);
    std::string lower_type = StringUtils::to_lower(type);

    std::smatch match;
    // varchar/binary/nchar/geometry(len)
    static const std::regex varlen_regex(R"((varchar|binary|nchar|varbinary|geometry)\s*\(\s*(\d+)\s*\))", std::regex::icase);
    // decimal(precision, scale)
    static const std::regex decimal_regex(R"(decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\))", std::regex::icase);

    if (std::regex_match(lower_type, match, varlen_regex)) {
        // Variable length type
        std::string base = match[1].str();
        int len_val = std::stoi(match[2].str());
        type_tag = get_type_tag(base);
        len = len_val;
        precision.reset();
        scale.reset();
        return;
    }
    if (std::regex_match(lower_type, match, decimal_regex)) {
        // decimal(precision, scale)
        type_tag = ColumnTypeTag::DECIMAL;
        precision = std::stoi(match[1].str());
        scale = std::stoi(match[2].str());
        len.reset();
        return;
    }

    // Compatible with decimal(precision)
    static const std::regex decimal1_regex(R"(decimal\s*\(\s*(\d+)\s*\))", std::regex::icase);
    if (std::regex_match(lower_type, match, decimal1_regex)) {
        type_tag = ColumnTypeTag::DECIMAL;
        precision = std::stoi(match[1].str());
        scale.reset();
        len.reset();
        return;
    }

    // Other types (no parameters)
    type_tag = get_type_tag(lower_type);
    if (type_tag == ColumnTypeTag::VARCHAR
        || type_tag == ColumnTypeTag::BINARY
        || type_tag == ColumnTypeTag::NCHAR
        || type_tag == ColumnTypeTag::VARBINARY
        || type_tag == ColumnTypeTag::GEOMETRY) {

        throw std::runtime_error("Variable length types must specify length: " + lower_type);
    } else if (type_tag == ColumnTypeTag::DECIMAL) {
        // DECIMAL type requires precision and scale
        if (!precision.has_value() || !scale.has_value()) {
            throw std::runtime_error("DECIMAL type must specify precision and scale: " + lower_type);
        }
    } else if (type_tag == ColumnTypeTag::JSON) {
        len = 200;
    } else {
        len.reset();
    }

    precision.reset();
    scale.reset();
}

bool ColumnConfig::is_var_length() const noexcept {
    static constexpr bool varlen_flags[] = {
        false, // UNKNOWN
        false, // BOOL
        false, // TINYINT
        false, // TINYINT_UNSIGNED
        false, // SMALLINT
        false, // SMALLINT_UNSIGNED
        false, // INT
        false, // INT_UNSIGNED
        false, // BIGINT
        false, // BIGINT_UNSIGNED
        false, // FLOAT
        false, // DOUBLE
        false, // DECIMAL
        true,  // NCHAR
        true,  // VARCHAR
        true,  // BINARY
        true,  // JSON
        true,  // VARBINARY
        true   // GEOMETRY
    };
    static_assert(sizeof(varlen_flags)/sizeof(varlen_flags[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "varlen_flags size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(varlen_flags)/sizeof(varlen_flags[0])) {
        return varlen_flags[idx];
    }
    return false;
}

size_t ColumnConfig::get_fixed_type_size() const noexcept{
    // DECIMAL special handling
    if (type_tag == ColumnTypeTag::DECIMAL) {
        if (precision && *precision <= 18)
            return sizeof(int64_t);      // DECIMAL64
        else
            return sizeof(int64_t) * 2;  // DECIMAL128
    }

    // Static lookup table
    static constexpr size_t type_sizes[] = {
        0,                 // UNKNOWN
        sizeof(bool),      // BOOL
        sizeof(int8_t),    // TINYINT
        sizeof(int8_t),    // TINYINT_UNSIGNED
        sizeof(int16_t),   // SMALLINT
        sizeof(int16_t),   // SMALLINT_UNSIGNED
        sizeof(int32_t),   // INT
        sizeof(int32_t),   // INT_UNSIGNED
        sizeof(int64_t),   // BIGINT
        sizeof(int64_t),   // BIGINT_UNSIGNED
        sizeof(float),     // FLOAT
        sizeof(double),    // DOUBLE
        0,                 // DECIMAL (handled separately)
        0,                 // NCHAR (variable length)
        0,                 // VARCHAR (variable length)
        0,                 // BINARY (variable length)
        0,                 // JSON (variable length)
        0,                 // VARBINARY (variable length)
        0                  // GEOMETRY (variable length)
    };
    static_assert(sizeof(type_sizes)/sizeof(type_sizes[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "type_sizes size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(type_sizes)/sizeof(type_sizes[0]) && type_sizes[idx] > 0) {
        return type_sizes[idx];
    }
    return 0;
}

// Convert to TDengine internal type
int ColumnConfig::get_taos_type() const noexcept {
    // DECIMAL special handling
    if (type_tag == ColumnTypeTag::DECIMAL) {
        return (precision && *precision <= 18) ? 
            TSDB_DATA_TYPE_DECIMAL64 : 
            TSDB_DATA_TYPE_DECIMAL;
    }

    // Static lookup table
    static constexpr int taos_types[] = {
        TSDB_DATA_TYPE_NULL,      // UNKNOWN
        TSDB_DATA_TYPE_BOOL,      // BOOL
        TSDB_DATA_TYPE_TINYINT,   // TINYINT
        TSDB_DATA_TYPE_UTINYINT,  // TINYINT_UNSIGNED
        TSDB_DATA_TYPE_SMALLINT,  // SMALLINT
        TSDB_DATA_TYPE_USMALLINT, // SMALLINT_UNSIGNED
        TSDB_DATA_TYPE_INT,       // INT
        TSDB_DATA_TYPE_UINT,      // INT_UNSIGNED
        TSDB_DATA_TYPE_BIGINT,    // BIGINT
        TSDB_DATA_TYPE_UBIGINT,   // BIGINT_UNSIGNED
        TSDB_DATA_TYPE_FLOAT,     // FLOAT
        TSDB_DATA_TYPE_DOUBLE,    // DOUBLE
        0,                        // DECIMAL (handled separately)
        TSDB_DATA_TYPE_NCHAR,     // NCHAR
        TSDB_DATA_TYPE_VARCHAR,   // VARCHAR
        TSDB_DATA_TYPE_BINARY,    // BINARY (treated as VARCHAR in TDengine)
        TSDB_DATA_TYPE_JSON,      // JSON
        TSDB_DATA_TYPE_VARBINARY, // VARBINARY
        TSDB_DATA_TYPE_GEOMETRY   // GEOMETRY
    };
    static_assert(sizeof(taos_types)/sizeof(taos_types[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "taos_types size mismatch");

    size_t idx = static_cast<size_t>(type_tag);
    if (idx < sizeof(taos_types)/sizeof(taos_types[0]) && taos_types[idx] != 0) {
        return taos_types[idx];
    }
    return TSDB_DATA_TYPE_NULL;
}
