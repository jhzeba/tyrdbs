#pragma once


#include <common/disallow_copy.h>
#include <common/exception.h>
#include <common/branch_prediction.h>

#include <memory>
#include <cstdint>
#include <cassert>


namespace tyrtech {


template<typename BufferType, typename SinkType = std::void_t<>>
class buffered_writer : private disallow_copy
{
public:
    DEFINE_EXCEPTION(runtime_error_exception, exception);

public:
    void write(const char* data, uint32_t size)
    {
        while (size != 0)
        {
            if (unlikely(m_offset == m_buffer->size()))
            {
                flush();

                if (unlikely(m_offset == m_buffer->size()))
                {
                    throw exception("cannot write data to sink");
                }
            }

            uint32_t part_size = std::min(static_cast<uint32_t>(m_buffer->size() - m_offset),
                                          size);

            if (part_size != 0)
            {
                std::memcpy(m_buffer->data() + m_offset, data, part_size);

                data += part_size;
                size -= part_size;

                m_offset += part_size;
            }
        }
    }

    template<typename T>
    void write(const T& data)
    {
        write(reinterpret_cast<const char*>(&data), sizeof(T));
    }

    template<typename T = SinkType>
    void flush(typename std::enable_if<!std::is_void<T>::value, bool>::type has_sink = true)
    {
        if (m_offset == 0)
        {
            return;
        }

        auto res = m_sink->write(m_buffer->data(), m_offset);

        if (unlikely(res != m_offset))
        {
            throw exception("cannot write data to sink");
        }

        m_offset = 0;
    }

    template<typename T = SinkType>
    void flush(typename std::enable_if<std::is_void<T>::value, bool>::type has_sink = false)
    {
    }

    template<typename T = SinkType>
    uint64_t offset(typename std::enable_if<!std::is_void<T>::value, bool>::type has_sink = true) const
    {
        return m_offset + m_sink->offset();
    }

    template<typename T = SinkType>
    uint64_t offset(typename std::enable_if<std::is_void<T>::value, bool>::type has_sink = false) const
    {
        return m_offset;
    }

public:
    buffered_writer(BufferType* buffer, SinkType* sink = nullptr) noexcept
      : m_buffer(buffer)
      , m_sink(sink)
    {
    }

private:
    uint32_t m_offset{0};

    BufferType* m_buffer{nullptr};
    SinkType* m_sink{nullptr};
};

}
