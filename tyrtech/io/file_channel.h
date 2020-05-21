#pragma once


#include <io/channel.h>
#include <io/file.h>


namespace tyrtech::io {


class file_channel : public channel
{
public:
    uint32_t write(const char* data, uint32_t size) override;
    uint32_t read(char* data, uint32_t size) override;

    uint64_t offset() const override;

    void set_offset(uint64_t offset);

public:
    file_channel(file* file);

private:
    file* m_file{nullptr};

    uint64_t m_offset{0};
};

}
