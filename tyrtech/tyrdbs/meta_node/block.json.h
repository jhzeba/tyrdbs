#pragma once


#include <message/builder.h>
#include <message/parser.h>


namespace tyrtech::tyrdbs::meta_node {


struct slice_builder final : public tyrtech::message::struct_builder<2, 12>
{
    slice_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_flags(uint16_t value)
    {
        *reinterpret_cast<uint16_t*>(m_static + 0) = value;
    }

    void set_ushard(uint16_t value)
    {
        *reinterpret_cast<uint16_t*>(m_static + 2) = value;
    }

    void set_size(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 4) = value;
    }

    void add_tid(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<2, 12>::add_value(value);
    }

    static constexpr uint16_t tid_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }

    void add_id(const std::string_view& value)
    {
        set_offset<1>();
        struct_builder<2, 12>::add_value(value);
    }

    static constexpr uint16_t id_bytes_required()
    {
        return tyrtech::message::element<std::string_view>::size;
    }
};

struct slice_parser final : public tyrtech::message::struct_parser<2, 12>
{
    slice_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    slice_parser() = default;

    decltype(auto) flags() const
    {
        return *reinterpret_cast<const uint16_t*>(m_static + 0);
    }

    decltype(auto) ushard() const
    {
        return *reinterpret_cast<const uint16_t*>(m_static + 2);
    }

    decltype(auto) size() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 4);
    }

    bool has_tid() const
    {
        return has_offset<0>();
    }

    decltype(auto) tid() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
    }

    bool has_id() const
    {
        return has_offset<1>();
    }

    decltype(auto) id() const
    {
        return tyrtech::message::element<std::string_view>().parse(m_parser, offset<1>());
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
