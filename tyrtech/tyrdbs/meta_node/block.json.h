#pragma once


#include <message/builder.h>
#include <message/parser.h>


namespace tyrtech::tyrdbs::meta_node {


struct slice_builder final : public tyrtech::message::struct_builder<0, 32>
{
    slice_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_id(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 0) = value;
    }

    void set_tid(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 8) = value;
    }

    void set_size(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 16) = value;
    }

    void set_key_count(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 24) = value;
    }
};

struct slice_parser final : public tyrtech::message::struct_parser<0, 32>
{
    slice_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    slice_parser() = default;

    decltype(auto) id() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 0);
    }

    decltype(auto) tid() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 8);
    }

    decltype(auto) size() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 16);
    }

    decltype(auto) key_count() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 24);
    }
};

struct block_builder final : public tyrtech::message::struct_builder<1, 1>
{
    struct slices_builder final : public tyrtech::message::list_builder
    {
        slices_builder(tyrtech::message::builder* builder)
          : list_builder(builder)
        {
        }

        decltype(auto) add_value()
        {
            add_element();
            return slice_builder(m_builder);
        }
    };

    block_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_flags(uint8_t value)
    {
        *reinterpret_cast<uint8_t*>(m_static + 0) = value;
    }

    decltype(auto) add_slices()
    {
        set_offset<0>();
        return slices_builder(m_builder);
    }

    static constexpr uint16_t slices_bytes_required()
    {
        return slices_builder::bytes_required();
    }
};

struct block_parser final : public tyrtech::message::struct_parser<1, 1>
{
    struct slices_parser final : public tyrtech::message::list_parser
    {
        slices_parser(const tyrtech::message::parser* parser, uint16_t offset)
          : list_parser(parser, offset)
        {
        }

        bool next()
        {
            if (m_elements == 0)
            {
                return false;
            }

            m_elements--;

            m_offset += m_element_size;

            m_element_size = tyrtech::message::element<uint16_t>().parse(m_parser, m_offset);
            m_element_size += tyrtech::message::element<uint16_t>::size;

            return true;
        }

        decltype(auto) value() const
        {
            return slice_parser(m_parser, m_offset);
        }
    };

    block_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    block_parser() = default;

    decltype(auto) flags() const
    {
        return *reinterpret_cast<const uint8_t*>(m_static + 0);
    }

    bool has_slices() const
    {
        return has_offset<0>();
    }

    decltype(auto) slices() const
    {
        return slices_parser(m_parser, offset<0>());
    }
};

}
