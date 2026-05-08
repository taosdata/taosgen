#include "InfluxDBClient.hpp"
#include "LogUtils.hpp"
#include <curl/curl.h>
#include <stdexcept>
#include <vector>
#include <zlib.h>

// Callback to capture response body
static size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* response = static_cast<std::string*>(userdata);
    response->append(ptr, size * nmemb);
    return size * nmemb;
}

InfluxDBClient::InfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options)
    : config_(config), format_options_(format_options) {
    write_url_ = build_write_url();
    auth_header_ = build_auth_header();
}

InfluxDBClient::~InfluxDBClient() {
    close();
}

std::string InfluxDBClient::build_write_url() const {
    // POST /api/v2/write?org={org}&bucket={bucket}&precision={precision}
    std::string url = config_.url;
    if (!url.empty() && url.back() == '/') {
        url.pop_back();
    }
    url += "/api/v2/write?org=" + config_.org +
           "&bucket=" + config_.bucket +
           "&precision=" + format_options_.precision;
    return url;
}

std::string InfluxDBClient::build_auth_header() const {
    return "Token " + config_.token;
}

bool InfluxDBClient::connect() {
    if (is_connected_) return true;

    curl_ = curl_easy_init();
    if (!curl_) {
        LogUtils::error("InfluxDB: failed to initialize curl");
        return false;
    }

    // Set common options
    curl_easy_setopt(curl_, CURLOPT_URL, write_url_.c_str());
    curl_easy_setopt(curl_, CURLOPT_POST, 1L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPIDLE, 60L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPINTVL, 30L);
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT, 10L);

    is_connected_ = true;
    LogUtils::debug("InfluxDB client connected to: {}", write_url_);
    return true;
}

bool InfluxDBClient::is_connected() const {
    return is_connected_;
}

void InfluxDBClient::close() {
    if (curl_) {
        curl_easy_cleanup(curl_);
        curl_ = nullptr;
    }
    is_connected_ = false;
}

bool InfluxDBClient::execute(const InfluxDBInsertData& data) {
    if (!curl_) {
        throw std::runtime_error("InfluxDB client not connected");
    }

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, ("Authorization: " + auth_header_).c_str());
    headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");

    const char* post_data = data.lines.c_str();
    long post_size = static_cast<long>(data.lines.size());

    // Optional gzip compression
    std::vector<uint8_t> compressed;
    if (format_options_.gzip && !data.lines.empty()) {
        headers = curl_slist_append(headers, "Content-Encoding: gzip");

        uLongf compressed_size = compressBound(data.lines.size());
        compressed.resize(compressed_size + 18); // gzip header/trailer overhead

        z_stream zs{};
        // deflateInit2 with gzip encoding (windowBits = 15 + 16)
        if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            curl_slist_free_all(headers);
            LogUtils::error("InfluxDB: gzip deflateInit2 failed");
            return false;
        }

        zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(data.lines.data()));
        zs.avail_in = static_cast<uInt>(data.lines.size());
        zs.next_out = compressed.data();
        zs.avail_out = static_cast<uInt>(compressed.size());

        int ret = deflate(&zs, Z_FINISH);
        deflateEnd(&zs);

        if (ret != Z_STREAM_END) {
            curl_slist_free_all(headers);
            LogUtils::error("InfluxDB: gzip compression failed");
            return false;
        }

        compressed.resize(zs.total_out);
        post_data = reinterpret_cast<const char*>(compressed.data());
        post_size = static_cast<long>(compressed.size());
    }

    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, post_size);
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, post_data);

    // Capture response
    std::string response_body;
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_body);

    CURLcode res = curl_easy_perform(curl_);
    curl_slist_free_all(headers);

    if (res != CURLE_OK) {
        LogUtils::error("InfluxDB write failed (curl error): {}", curl_easy_strerror(res));
        return false;
    }

    long http_code = 0;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code);

    if (http_code == 204) {
        return true;
    }

    // Error: InfluxDB returns non-204
    std::string preview = response_body.size() > 200
        ? response_body.substr(0, 200) + "..."
        : response_body;
    LogUtils::error("InfluxDB write failed (HTTP {}): {}", http_code, preview);
    return false;
}
