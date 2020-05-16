#pragma once


#include <cstdint>


namespace tyrtech::io {


class channel
{
public:
    virtual uint32_t write(const char* data, uint32_t size) = 0;
    virtual uint32_t read(char* data, uint32_t size) = 0;

    virtual uint64_t offset() const = 0;
    virtual void set_offset(uint64_t offset) = 0;

public:
    virtual ~channel() = default;
};

}
