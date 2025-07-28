#pragma once

#include <string>
#include <variant>
#include <vector>
#include "SqlInsertData.h"
#include "StmtV2InsertData.h"



// General format result type
using FormatResult = std::variant<std::string, std::vector<std::string>, SqlInsertData, StmtV2InsertData>;