#include <common/branch_prediction.h>
#include <common/conv.h>

#include <charconv>


namespace tyrtech::conv {


template<>
uint16_t parse(const std::string_view& value)
{
    uint16_t parsed_value;

    auto res = std::from_chars(value.begin(), value.end(), parsed_value);

    if (unlikely(res.ptr != value.end() || res.ec != std::errc()))
    {
        throw format_error_exception("can't convert '{}' to u16", value);
    }

    return parsed_value;
}

template<>
uint32_t parse(const std::string_view& value)
{
    uint32_t parsed_value;

    auto res = std::from_chars(value.begin(), value.end(), parsed_value);

    if (unlikely(res.ptr != value.end() || res.ec != std::errc()))
    {
        throw format_error_exception("can't convert '{}' to u32", value);
    }

    return parsed_value;
}

template<>
uint64_t parse(const std::string_view& value)
{
    uint64_t parsed_value;

    auto res = std::from_chars(value.begin(), value.end(), parsed_value);

    if (unlikely(res.ptr != value.end() || res.ec != std::errc()))
    {
        throw format_error_exception("can't convert '{}' to u64", value);
    }

    return parsed_value;
}

template<>
double parse(const std::string_view& value)
{
    char buff[32];

    if (unlikely(value.size() >= sizeof(buff)))
    {
        throw format_error_exception("can't convert '{}' to double", value);
    }

    std::memcpy(buff, value.data(), value.size());
    buff[value.size()] = 0;

    char* end_ptr{nullptr};
    double parsed_value = std::strtod(buff, &end_ptr);

    if (unlikely(end_ptr != (buff + value.size()) || errno != 0))
    {
        throw format_error_exception("can't convert '{}' to double", value);
    }

    return parsed_value;
}

template<>
std::string_view parse(const std::string_view& value)
{
    return value;
}

}
