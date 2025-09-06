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


enum class InsertMode {
    SubTable,      // INSERT INTO ? VALUES(?,cols-qmark)
    SuperTable,    // INSERT INTO `db_name`.`stb_name`(tbname,ts,cols-name) VALUES(?,?,col-qmark)
    AutoCreate     // INSERT INTO ? USING `db_name`.`stb_name` TAGS (tags-qmark) VALUES(?,cols-qmark)
};

class IInsertDataFormatter : public IFormatter {
public:
    virtual std::string prepare(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances) = 0;
    virtual FormatResult format(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, MemoryPool::MemoryBlock* batch, bool is_checkpoint_recover = false) const = 0;

protected:
    InsertMode mode_;
};

