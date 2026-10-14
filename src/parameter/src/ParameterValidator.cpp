/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

class ParameterValidator {
public:
    // Unknown parameter detection
    static void check_unknown_params(
        const std::set<std::string>& input_params,
        const std::map<std::string, ParamDescriptor>& descriptors);
    
    // Conditional dependency validation
    static void check_conditional_params(
        const ParameterContext& context,
        const std::string& trigger_param,
        const std::set<std::string>& required_params);
    
    // Type compatibility check
    static bool check_type_compatibility(
        const std::string& param_name,
        const ParamValue& actual_value,
        const ParamDescriptor& descriptor);
};

// void validate_filetype_dependent(const ParameterContext& ctx) {
//   if (ctx.get<std::string>("filetype") == "csvfile") {
//       if (!ctx.has("super_tables.csv_ts_format")) {
//           throw ConfigError("csv_ts_format required when filetype=csvfile");
//       }
//   }
// }