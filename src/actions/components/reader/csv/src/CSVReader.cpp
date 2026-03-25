#include "CSVReader.hpp"
#include "StringUtils.hpp"
#include "LogUtils.hpp"
#include <csv.hpp>
#include <cerrno>
#include <cstring>
#include <stdexcept>

struct CSVReader::Impl {
    std::vector<std::string> file_paths;
    bool has_header;
    char delimiter;
    size_t column_count = 0;

    // Multi-file state
    size_t current_file_index = 0;
    std::unique_ptr<csv::CSVReader> current_reader;
    csv::CSVReader::iterator current_it;
    bool need_skip_header = false;  // For subsequent files with headers

    Impl(const std::vector<std::string>& paths, bool header, char delim)
        : file_paths(paths), has_header(header), delimiter(delim) {
        if (file_paths.empty()) {
            throw std::runtime_error("No CSV file paths provided");
        }
        // Open first file to determine column count
        open_file(0);
        column_count = determine_column_count();
        // Re-open to reset position
        open_file(0);
    }

    csv::CSVFormat make_format(bool skip_header_as_names) const {
        csv::CSVFormat format;
        format.delimiter(delimiter);
        if (!skip_header_as_names) {
            format.no_header();
        }
        format.trim({' ', '\t'});
        return format;
    }

    size_t determine_column_count() {
        if (has_header) {
            return current_reader->get_col_names().size();
        }
        // For no-header files, read first row to get column count
        auto it = current_reader->begin();
        if (it != current_reader->end()) {
            return (*it).size();
        }
        return 0;
    }

    void open_file(size_t index) {
        current_file_index = index;
        if (index >= file_paths.size()) {
            current_reader.reset();
            return;
        }
        // First file: use header mode if has_header
        // Subsequent files: always no_header (header line becomes data, we skip it)
        bool use_header_mode = (index == 0 && has_header);
        auto format = make_format(use_header_mode);
        current_reader = std::make_unique<csv::CSVReader>(file_paths[index], format);
        current_it = current_reader->begin();
        need_skip_header = (index > 0 && has_header);
    }

    std::optional<CSVRow> next() {
        while (current_reader) {
            // Skip header row of subsequent files
            if (need_skip_header) {
                need_skip_header = false;
                if (current_it != current_reader->end()) {
                    ++current_it;
                }
            }

            if (current_it != current_reader->end()) {
                csv::CSVRow& row = *current_it;

                // Validate column count consistency across files
                if (row.size() != column_count) {
                    throw std::runtime_error(
                        "Column count mismatch in file '" + file_paths[current_file_index] +
                        "': expected " + std::to_string(column_count) +
                        " but got " + std::to_string(row.size()));
                }

                CSVRow result = row_to_csvrow(row);
                ++current_it;
                return result;
            }

            // Current file exhausted, move to next
            size_t next_idx = current_file_index + 1;
            if (next_idx < file_paths.size()) {
                LogUtils::debug("Switching to next CSV file: {}", file_paths[next_idx]);
                open_file(next_idx);
            } else {
                current_reader.reset();
                return std::nullopt;
            }
        }
        return std::nullopt;
    }

    static CSVRow row_to_csvrow(csv::CSVRow& row) {
        CSVRow result;
        result.reserve(row.size());
        for (auto& field : row) {
            std::string val = field.get<>();
            StringUtils::trim(val);
            if (val.size() >= 2) {
                char first = val.front();
                char last = val.back();
                if ((first == last) && (first == '"' || first == '\'')) {
                    val.pop_back();
                    val.erase(0, 1);
                }
            }
            result.push_back(std::move(val));
        }
        return result;
    }
};

CSVReader::CSVReader(const std::string& file_path, bool has_header, char delimiter)
    : CSVReader(std::vector<std::string>{file_path}, has_header, delimiter) {
}

CSVReader::CSVReader(const std::vector<std::string>& file_paths, bool has_header, char delimiter)
    : impl_(std::make_unique<Impl>(file_paths, has_header, delimiter)) {
}

CSVReader::CSVReader(CSVReader&&) noexcept = default;
CSVReader& CSVReader::operator=(CSVReader&&) noexcept = default;
CSVReader::~CSVReader() = default;

std::vector<CSVRow> CSVReader::read_all() {
    reset();
    std::vector<CSVRow> rows;
    while (auto row = read_next()) {
        rows.push_back(std::move(*row));
    }
    return rows;
}

std::optional<CSVRow> CSVReader::read_next() {
    return impl_->next();
}

size_t CSVReader::column_count() const {
    return impl_->column_count;
}

void CSVReader::reset() {
    impl_->open_file(0);
}