#pragma once


#include <message/builder.h>
#include <message/parser.h>


namespace tyrtech::tyrdbs::meta_node {


struct entry_builder final : public tyrtech::message::struct_builder<2, 1>
{
    entry_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_flags(uint8_t value)
    {
        *reinterpret_cast<uint8_t*>(m_static + 0) = value;
    }

    void add_key(const std::string_view& value)
    {
        set_offset<0>();
        struct_builder<2, 1>::add_value(value);
    }

    static constexpr uint16_t key_bytes_required()
    {
        return tyrtech::message::element<std::string_view>::size;
    }

    void add_value(const std::string_view& value)
    {
        set_offset<1>();
        struct_builder<2, 1>::add_value(value);
    }

    static constexpr uint16_t value_bytes_required()
    {
        return tyrtech::message::element<std::string_view>::size;
    }
};

struct entry_parser final : public tyrtech::message::struct_parser<2, 1>
{
    entry_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    entry_parser() = default;

    decltype(auto) flags() const
    {
        return *reinterpret_cast<const uint8_t*>(m_static + 0);
    }

    bool has_key() const
    {
        return has_offset<0>();
    }

    decltype(auto) key() const
    {
        return tyrtech::message::element<std::string_view>().parse(m_parser, offset<0>());
    }

    bool has_value() const
    {
        return has_offset<1>();
    }

    decltype(auto) value() const
    {
        return tyrtech::message::element<std::string_view>().parse(m_parser, offset<1>());
    }
};

struct ushard_builder final : public tyrtech::message::struct_builder<1, 4>
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

    ushard_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_id(uint32_t value)
    {
        *reinterpret_cast<uint32_t*>(m_static + 0) = value;
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

struct ushard_parser final : public tyrtech::message::struct_parser<1, 4>
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

    ushard_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    ushard_parser() = default;

    decltype(auto) id() const
    {
        return *reinterpret_cast<const uint32_t*>(m_static + 0);
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

struct data_builder final : public tyrtech::message::struct_builder<1, 1>
{
    struct ushards_builder final : public tyrtech::message::list_builder
    {
        ushards_builder(tyrtech::message::builder* builder)
          : list_builder(builder)
        {
        }

        decltype(auto) add_value()
        {
            add_element();
            return ushard_builder(m_builder);
        }
    };

    data_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_flags(uint8_t value)
    {
        *reinterpret_cast<uint8_t*>(m_static + 0) = value;
    }

    decltype(auto) add_ushards()
    {
        set_offset<0>();
        return ushards_builder(m_builder);
    }

    static constexpr uint16_t ushards_bytes_required()
    {
        return ushards_builder::bytes_required();
    }
};

struct data_parser final : public tyrtech::message::struct_parser<1, 1>
{
    struct ushards_parser final : public tyrtech::message::list_parser
    {
        ushards_parser(const tyrtech::message::parser* parser, uint16_t offset)
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
            return ushard_parser(m_parser, m_offset);
        }
    };

    data_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    data_parser() = default;

    decltype(auto) flags() const
    {
        return *reinterpret_cast<const uint8_t*>(m_static + 0);
    }

    bool has_ushards() const
    {
        return has_offset<0>();
    }

    decltype(auto) ushards() const
    {
        return ushards_parser(m_parser, offset<0>());
    }
};

}
