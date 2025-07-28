#include "RandomColumnGenerator.h"
#include <random>
#include <stdexcept>
#include <functional>
#include "pcg_random.hpp"


// static thread_local std::mt19937_64 random_engine(std::random_device{}());
// static thread_local std::minstd_rand random_engine(std::random_device{}());
static thread_local pcg32_fast random_engine(pcg_extras::seed_seq_from<std::random_device>{});


static ColumnType random_string_column(const RandomColumnGenerator& gen) {
    if (gen.instance_.config().corpus) {
        const auto& corpus = gen.instance_.config().corpus.value();
        std::uniform_int_distribution<size_t> dist(0, corpus.size() - 1);
        return std::string(1, corpus[dist(random_engine)]);
    } else {
        static const std::string default_corpus = "abcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<size_t> dist(0, default_corpus.size() - 1);

        std::string result;
        result.reserve(*gen.instance_.config().len);
        for (int i = 0; i < *gen.instance_.config().len; ++i) {
            result.push_back(default_corpus[dist(random_engine)]);
        }
        return result;
    }
}

using GenFunc = std::function<ColumnType(const RandomColumnGenerator&)>;
static const GenFunc generators[] = {
    // UNKNOWN
    nullptr,

    // BOOL
    [](const RandomColumnGenerator& gen) {
        (void)gen;
        std::bernoulli_distribution dist(0.5);
        return dist(random_engine);
    },

    // TINYINT
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<int8_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // TINYINT_UNSIGNED
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<uint8_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // SMALLINT
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<int16_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // SMALLINT_UNSIGNED
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<uint16_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // INT
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<int32_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // INT_UNSIGNED
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<uint32_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // BIGINT
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<int64_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // BIGINT_UNSIGNED
    [](const RandomColumnGenerator& gen) {
        std::uniform_int_distribution<uint64_t> dist(*gen.instance_.config().min, *gen.instance_.config().max - 1);
        return dist(random_engine);
    },

    // FLOAT
    [](const RandomColumnGenerator& gen) {
        std::uniform_real_distribution<float> dist(*gen.instance_.config().min, *gen.instance_.config().max);
        return dist(random_engine);
    },

    // DOUBLE
    [](const RandomColumnGenerator& gen) {
        std::uniform_real_distribution<double> dist(*gen.instance_.config().min, *gen.instance_.config().max);
        return dist(random_engine);
    },

    // DECIMAL
    [](const RandomColumnGenerator& gen) -> ColumnType {
        (void)gen;
        throw std::runtime_error("DECIMAL type random generation not implemented");
        return ColumnType();
    },

    // NCHAR
    [](const RandomColumnGenerator& gen) -> ColumnType {
        int len = *gen.instance_.config().len;
        std::uniform_int_distribution<char16_t> dist(0x4E00, 0x9FA5); // Unicode range for Chinese characters
        std::u16string result;
        result.reserve(len);
        for (int i = 0; i < len; ++i) {
            result.push_back(dist(random_engine));
        }
        return result;
    },

    // VARCHAR
    random_string_column,

    // BINARY
    random_string_column,

    // JSON
    [](const RandomColumnGenerator& gen) -> ColumnType {
        (void)gen;
        throw std::runtime_error("JSON type random generation not implemented");
        return ColumnType();
    },

    // VARBINARY
    [](const RandomColumnGenerator& gen) -> ColumnType {
        (void)gen;
        throw std::runtime_error("VARBINARY type random generation not implemented");
        return ColumnType();
    },

    // GEOMETRY
    [](const RandomColumnGenerator& gen) -> ColumnType {
        (void)gen;
        throw std::runtime_error("GEOMETRY type random generation not implemented");
        return ColumnType();
    },
};


ColumnType RandomColumnGenerator::generate() const {
    static_assert(sizeof(generators)/sizeof(generators[0]) == static_cast<size_t>(ColumnTypeTag::MAX), "generators size mismatch");

    size_t idx = static_cast<size_t>(instance_.config().type_tag);
    return generators[idx](*this);
}

ColumnTypeVector RandomColumnGenerator::generate(size_t count) const {
    ColumnTypeVector values;
    values.reserve(count);
    
    for (size_t i = 0; i < count; ++i) {
        values.push_back(generate());
    }
    
    return values;
}