#pragma once


#include <io/file.h>


namespace tyrtech::io {


class file_channel
{
public:
    virtual uint32_t pread(uint64_t offset, char* data, uint32_t size) const;

    virtual uint32_t write(const char* data, uint32_t size);
    virtual uint64_t offset() const;

public:
    file_channel(file* file);
    virtual ~file_channel() = default;

private:
    file* m_file{nullptr};
    uint64_t m_offset{0};
};

}
