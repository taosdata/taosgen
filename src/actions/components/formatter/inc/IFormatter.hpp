#pragma once
#include <variant>
#include "ActionConfigVariant.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableData.hpp"
#include "FormatResult.hpp"


class IFormatter {
public:
    virtual ~IFormatter() = default;
};


class IDatabaseFormatter : public IFormatter {
public:
    virtual FormatResult format(const CreateDatabaseConfig&) const = 0;
};


class ISuperTableFormatter : public IFormatter {
public:
    virtual FormatResult format(const CreateSuperTableConfig&) const = 0;
};


class IChildTableFormatter : public IFormatter {
    public:
        virtual FormatResult format(const CreateChildTableConfig& config, 
                                    const std::vector<std::string>& table_names, 
                                    const std::vector<RowType>& tags) const = 0;
    };


class IInsertDataFormatter : public IFormatter {
public:
    virtual std::string prepare(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, int mode) const = 0;
    virtual FormatResult format(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, MemoryPool::MemoryBlock* batch) const = 0;
};

