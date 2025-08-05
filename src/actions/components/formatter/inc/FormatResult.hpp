#pragma once
#include "SqlInsertData.hpp"
#include "StmtV2InsertData.hpp"
#include <string>
#include <variant>
#include <vector>



// General format result type
using FormatResult = std::variant<std::string, std::vector<std::string>, SqlInsertData, StmtV2InsertData>;