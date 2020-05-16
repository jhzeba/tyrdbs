#pragma once


#include <common/disallow_copy.h>
#include <common/exception.h>
#include <common/branch_prediction.h>

#include <memory>
#include <cassert>
#include <cstdint>


namespace tyrtech {


template<typename BufferType, typename SourceType = std::void_t<>>
class buffered_reader : private disallow_copy
{
public:
    DEFINE_EXCEPTION(runtime_error_exception, exception);

public:
    void read(char* data, uint32_t size)
    {
        while (size != 0)
        {
            if (unlikely(m_offset == m_size))
            {
                load();

                if (unlikely(m_offset == m_size))
                {
                    throw exception("no more data in source");
                }
            }

            uint32_t part_size = std::min(size, m_size - m_offset);

            std::memcpy(data, m_buffer->data() + m_offset, part_size);

            data += part_size;
            size -= part_size;

            m_offset += part_size;
        }
    }

    template<typename T>
    void read(T* data)
    {
        read(reinterpret_cast<char*>(data), sizeof(T));
    }

    template<typename T = SourceType>
    uint64_t offset(typename std::enable_if<!std::is_void<T>::value, bool>::type has_sink = true) const
    {
        return m_offset + m_source->offset();
    }

    template<typename T = SourceType>
    uint64_t offset(typename std::enable_if<std::is_void<T>::value, bool>::type has_sink = false) const
    {
        return m_offset;
    }

public:
    buffered_reader(BufferType* buffer, SourceType* source = nullptr) noexcept
      : m_buffer(buffer)
      , m_source(source)
    {
    }

private:
    uint32_t m_size{0};
    uint32_t m_offset{0};

    BufferType* m_buffer{nullptr};
    SourceType* m_source{nullptr};

private:
    template<typename T = SourceType>
    void load(typename std::enable_if<!std::is_void<T>::value, bool>::type has_sink = true)
    {
        m_size = m_source->read(m_buffer->data(), m_buffer->size());
        m_offset = 0;

        assert(likely(m_size != 0));
    }

    template<typename T = SourceType>
    void load(typename std::enable_if<std::is_void<T>::value, bool>::type has_sink = false)
    {
        m_size = m_buffer->size();
    }
};

}
