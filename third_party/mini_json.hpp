// ============================================================================
// mini_json.hpp - Lightweight single-header JSON library (C++17)
//
// The specification asks for "a lightweight JSON library such as
// nlohmann/json". nlohmann/json could not be fetched in the offline build
// environment, so this header provides an equivalent minimal JSON
// implementation: parse / serialize / object & array access.
//
// Supported: objects, arrays, strings (with escapes + \uXXXX), numbers
// (int64 / double), booleans, null. UTF-8 pass-through.
// ============================================================================
#pragma once

#include <cctype>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace json {

class Json {
public:
    enum class Type { Null, Bool, Int, Double, String, Array, Object };

    using Array  = std::vector<Json>;
    using Object = std::map<std::string, Json>;

    // ---- constructors ------------------------------------------------------
    Json() : type_(Type::Null) {}
    Json(std::nullptr_t) : type_(Type::Null) {}
    Json(bool b) : type_(Type::Bool), bool_(b) {}
    Json(int i) : type_(Type::Int), int_(i) {}
    Json(long i) : type_(Type::Int), int_(i) {}
    Json(long long i) : type_(Type::Int), int_(i) {}
    Json(unsigned long i) : type_(Type::Int), int_(static_cast<long long>(i)) {}
    Json(unsigned long long i) : type_(Type::Int), int_(static_cast<long long>(i)) {}
    Json(double d) : type_(Type::Double), double_(d) {}
    Json(const char* s) : type_(Type::String), str_(s) {}
    Json(const std::string& s) : type_(Type::String), str_(s) {}
    Json(std::string&& s) : type_(Type::String), str_(std::move(s)) {}
    Json(const Array& a) : type_(Type::Array), arr_(a) {}
    Json(const Object& o) : type_(Type::Object), obj_(o) {}

    static Json array()  { Json j; j.type_ = Type::Array;  return j; }
    static Json object() { Json j; j.type_ = Type::Object; return j; }

    // ---- type queries ------------------------------------------------------
    Type type() const { return type_; }
    bool is_null()   const { return type_ == Type::Null; }
    bool is_bool()   const { return type_ == Type::Bool; }
    bool is_number() const { return type_ == Type::Int || type_ == Type::Double; }
    bool is_string() const { return type_ == Type::String; }
    bool is_array()  const { return type_ == Type::Array; }
    bool is_object() const { return type_ == Type::Object; }

    // ---- value access ------------------------------------------------------
    bool as_bool() const {
        if (type_ != Type::Bool) throw std::runtime_error("json: not a bool");
        return bool_;
    }
    long long as_int() const {
        if (type_ == Type::Int) return int_;
        if (type_ == Type::Double) return static_cast<long long>(double_);
        throw std::runtime_error("json: not a number");
    }
    double as_double() const {
        if (type_ == Type::Int) return static_cast<double>(int_);
        if (type_ == Type::Double) return double_;
        throw std::runtime_error("json: not a number");
    }
    const std::string& as_string() const {
        if (type_ != Type::String) throw std::runtime_error("json: not a string");
        return str_;
    }

    // ---- container access --------------------------------------------------
    size_t size() const {
        if (type_ == Type::Array) return arr_.size();
        if (type_ == Type::Object) return obj_.size();
        return 0;
    }
    bool contains(const std::string& key) const {
        return type_ == Type::Object && obj_.count(key) > 0;
    }
    Json& operator[](const std::string& key) {
        if (type_ == Type::Null) type_ = Type::Object;
        if (type_ != Type::Object) throw std::runtime_error("json: not an object");
        return obj_[key];
    }
    const Json& operator[](const std::string& key) const {
        static const Json null_json;
        if (type_ != Type::Object) return null_json;
        auto it = obj_.find(key);
        return it == obj_.end() ? null_json : it->second;
    }
    Json& operator[](size_t i) {
        if (type_ != Type::Array) throw std::runtime_error("json: not an array");
        return arr_[i];
    }
    const Json& operator[](size_t i) const {
        if (type_ != Type::Array) throw std::runtime_error("json: not an array");
        return arr_[i];
    }
    void push_back(const Json& v) {
        if (type_ == Type::Null) type_ = Type::Array;
        if (type_ != Type::Array) throw std::runtime_error("json: not an array");
        arr_.push_back(v);
    }
    Array&  items()       { return arr_; }
    Object& members()     { return obj_; }
    const Array&  items()   const { return arr_; }
    const Object& members() const { return obj_; }

    // ---- serialization -----------------------------------------------------
    std::string dump(int indent = -1) const {
        std::string out;
        dump_to(out, indent, 0);
        return out;
    }

    // ---- parsing -----------------------------------------------------------
    static Json parse(const std::string& text) {
        Parser p(text);
        Json v = p.parse_value();
        p.skip_ws();
        if (!p.at_end()) throw std::runtime_error("json: trailing characters");
        return v;
    }

private:
    Type type_;
    bool bool_ = false;
    long long int_ = 0;
    double double_ = 0.0;
    std::string str_;
    Array arr_;
    Object obj_;

    static void append_indent(std::string& out, int indent, int depth) {
        if (indent < 0) return;
        out.push_back('\n');
        out.append(static_cast<size_t>(indent) * depth, ' ');
    }

    static void escape_string(const std::string& s, std::string& out) {
        out.push_back('"');
        for (unsigned char c : s) {
            switch (c) {
                case '"':  out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\b': out += "\\b";  break;
                case '\f': out += "\\f";  break;
                case '\n': out += "\\n";  break;
                case '\r': out += "\\r";  break;
                case '\t': out += "\\t";  break;
                default:
                    if (c < 0x20) {
                        char buf[8];
                        std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                        out += buf;
                    } else {
                        out.push_back(static_cast<char>(c));
                    }
            }
        }
        out.push_back('"');
    }

    void dump_to(std::string& out, int indent, int depth) const {
        switch (type_) {
            case Type::Null:   out += "null"; break;
            case Type::Bool:   out += bool_ ? "true" : "false"; break;
            case Type::Int:    out += std::to_string(int_); break;
            case Type::Double: {
                char buf[32];
                std::snprintf(buf, sizeof(buf), "%.17g", double_);
                out += buf;
                break;
            }
            case Type::String: escape_string(str_, out); break;
            case Type::Array: {
                if (arr_.empty()) { out += "[]"; break; }
                out.push_back('[');
                for (size_t i = 0; i < arr_.size(); ++i) {
                    if (i) out.push_back(',');
                    append_indent(out, indent, depth + 1);
                    arr_[i].dump_to(out, indent, depth + 1);
                }
                append_indent(out, indent, depth);
                out.push_back(']');
                break;
            }
            case Type::Object: {
                if (obj_.empty()) { out += "{}"; break; }
                out.push_back('{');
                bool first = true;
                for (const auto& kv : obj_) {
                    if (!first) out.push_back(',');
                    first = false;
                    append_indent(out, indent, depth + 1);
                    escape_string(kv.first, out);
                    out.push_back(':');
                    if (indent >= 0) out.push_back(' ');
                    kv.second.dump_to(out, indent, depth + 1);
                }
                append_indent(out, indent, depth);
                out.push_back('}');
                break;
            }
        }
    }

    class Parser {
    public:
        explicit Parser(const std::string& t) : t_(t) {}
        bool at_end() const { return pos_ >= t_.size(); }
        void skip_ws() {
            while (pos_ < t_.size() &&
                   (t_[pos_] == ' ' || t_[pos_] == '\t' ||
                    t_[pos_] == '\n' || t_[pos_] == '\r'))
                ++pos_;
        }
        char peek() {
            skip_ws();
            if (at_end()) throw std::runtime_error("json: unexpected end");
            return t_[pos_];
        }
        char get() {
            if (at_end()) throw std::runtime_error("json: unexpected end");
            return t_[pos_++];
        }
        void expect(char c) {
            skip_ws();
            if (get() != c) throw std::runtime_error("json: unexpected character");
        }
        void expect_literal(const char* lit) {
            for (const char* p = lit; *p; ++p)
                if (get() != *p) throw std::runtime_error("json: bad literal");
        }

        Json parse_value() {
            char c = peek();
            switch (c) {
                case '{': return parse_object();
                case '[': return parse_array();
                case '"': return Json(parse_string());
                case 't': expect_literal("true");  return Json(true);
                case 'f': expect_literal("false"); return Json(false);
                case 'n': expect_literal("null");  return Json();
                default:  return parse_number();
            }
        }

        Json parse_object() {
            expect('{');
            Json obj = Json::object();
            skip_ws();
            if (peek() == '}') { get(); return obj; }
            while (true) {
                skip_ws();
                if (peek() != '"') throw std::runtime_error("json: expected key");
                std::string key = parse_string();
                expect(':');
                obj.members()[key] = parse_value();
                skip_ws();
                char c = get();
                if (c == '}') break;
                if (c != ',') throw std::runtime_error("json: expected , or }");
            }
            return obj;
        }

        Json parse_array() {
            expect('[');
            Json arr = Json::array();
            skip_ws();
            if (peek() == ']') { get(); return arr; }
            while (true) {
                arr.push_back(parse_value());
                skip_ws();
                char c = get();
                if (c == ']') break;
                if (c != ',') throw std::runtime_error("json: expected , or ]");
            }
            return arr;
        }

        std::string parse_string() {
            expect('"');
            std::string out;
            while (true) {
                char c = get();
                if (c == '"') break;
                if (c == '\\') {
                    char e = get();
                    switch (e) {
                        case '"':  out.push_back('"');  break;
                        case '\\': out.push_back('\\'); break;
                        case '/':  out.push_back('/');  break;
                        case 'b':  out.push_back('\b'); break;
                        case 'f':  out.push_back('\f'); break;
                        case 'n':  out.push_back('\n'); break;
                        case 'r':  out.push_back('\r'); break;
                        case 't':  out.push_back('\t'); break;
                        case 'u': {
                            uint32_t cp = parse_hex4();
                            if (cp >= 0xD800 && cp <= 0xDBFF) {  // surrogate pair
                                if (get() != '\\' || get() != 'u')
                                    throw std::runtime_error("json: bad surrogate");
                                uint32_t lo = parse_hex4();
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                            }
                            encode_utf8(cp, out);
                            break;
                        }
                        default: throw std::runtime_error("json: bad escape");
                    }
                } else {
                    out.push_back(c);
                }
            }
            return out;
        }

        uint32_t parse_hex4() {
            uint32_t v = 0;
            for (int i = 0; i < 4; ++i) {
                char c = get();
                v <<= 4;
                if (c >= '0' && c <= '9') v |= static_cast<uint32_t>(c - '0');
                else if (c >= 'a' && c <= 'f') v |= static_cast<uint32_t>(c - 'a' + 10);
                else if (c >= 'A' && c <= 'F') v |= static_cast<uint32_t>(c - 'A' + 10);
                else throw std::runtime_error("json: bad \\u escape");
            }
            return v;
        }

        static void encode_utf8(uint32_t cp, std::string& out) {
            if (cp < 0x80) {
                out.push_back(static_cast<char>(cp));
            } else if (cp < 0x800) {
                out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else if (cp < 0x10000) {
                out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else {
                out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
        }

        Json parse_number() {
            skip_ws();
            size_t start = pos_;
            if (pos_ < t_.size() && (t_[pos_] == '-' || t_[pos_] == '+')) ++pos_;
            bool is_double = false;
            while (pos_ < t_.size()) {
                char c = t_[pos_];
                if (std::isdigit(static_cast<unsigned char>(c))) { ++pos_; continue; }
                if (c == '.' || c == 'e' || c == 'E' || c == '-' || c == '+') {
                    if (c == '.' || c == 'e' || c == 'E') is_double = true;
                    ++pos_; continue;
                }
                break;
            }
            if (pos_ == start) throw std::runtime_error("json: bad number");
            std::string num = t_.substr(start, pos_ - start);
            try {
                if (is_double) return Json(std::stod(num));
                return Json(static_cast<long long>(std::stoll(num)));
            } catch (...) {
                throw std::runtime_error("json: bad number");
            }
        }

    private:
        const std::string& t_;
        size_t pos_ = 0;
    };
};

}  // namespace json
