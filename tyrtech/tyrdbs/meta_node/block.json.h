#pragma once


#include <message/builder.h>
#include <message/parser.h>


namespace tyrtech::tyrdbs::meta_node {


struct entry_builder final : public tyrtech::message::struct_builder<3, 3>
{
    entry_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_ushard_id(uint16_t value)
    {
        *reinterpret_cast<uint16_t*>(m_static + 0) = value;
    }

    void set_flags(uint8_t value)
    {
        *reinterpret_cast<uint8_t*>(m_static + 2) = value;
    }

    void add_tid(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<3, 3>::add_value(value);
    }

    static constexpr uint16_t tid_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }

    void add_key(const std::string_view& value)
    {
        set_offset<1>();
        struct_builder<3, 3>::add_value(value);
    }

    static constexpr uint16_t key_bytes_required()
    {
        return tyrtech::message::element<std::string_view>::size;
    }

    void add_value(const std::string_view& value)
    {
        set_offset<2>();
        struct_builder<3, 3>::add_value(value);
    }

    static constexpr uint16_t value_bytes_required()
    {
        return tyrtech::message::element<std::string_view>::size;
    }
};

struct entry_parser final : public tyrtech::message::struct_parser<3, 3>
{
    entry_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    entry_parser() = default;

    decltype(auto) ushard_id() const
    {
        return *reinterpret_cast<const uint16_t*>(m_static + 0);
    }

    decltype(auto) flags() const
    {
        return *reinterpret_cast<const uint8_t*>(m_static + 2);
    }

    bool has_tid() const
    {
        return has_offset<0>();
    }

    decltype(auto) tid() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
    }

    bool has_key() const
    {
        return has_offset<1>();
    }

    decltype(auto) key() const
    {
        return tyrtech::message::element<std::string_view>().parse(m_parser, offset<1>());
    }

    bool has_value() const
    {
        return has_offset<2>();
    }

    decltype(auto) value() const
    {
        return tyrtech::message::element<std::string_view>().parse(m_parser, offset<2>());
    }
};

struct block_builder final : public tyrtech::message::struct_builder<1, 1>
{
    struct entries_builder final : public tyrtech::message::list_builder
    {
        entries_builder(tyrtech::message::builder* builder)
          : list_builder(builder)
        {
        }

        decltype(auto) add_value()
        {
            add_element();
            return entry_builder(m_builder);
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

    decltype(auto) add_entries()
    {
        set_offset<0>();
        return entries_builder(m_builder);
    }

    static constexpr uint16_t entries_bytes_required()
    {
        return entries_builder::bytes_required();
    }
};

struct block_parser final : public tyrtech::message::struct_parser<1, 1>
{
    struct entries_parser final : public tyrtech::message::list_parser
    {
        entries_parser(const tyrtech::message::parser* parser, uint16_t offset)
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
            return entry_parser(m_parser, m_offset);
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

    bool has_entries() const
    {
        return has_offset<0>();
    }

    decltype(auto) entries() const
    {
        return entries_parser(m_parser, offset<0>());
    }
};

}
