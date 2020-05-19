#pragma once


#include <net/socket_channel.h>
#include <net/server_exception.h>
#include <net/service.json.h>


namespace tyrtech::tyrdbs::meta_node::log {


namespace messages::fetch {


struct request_builder final : public tyrtech::message::struct_builder<0, 0>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }
};

struct request_parser final : public tyrtech::message::struct_parser<0, 0>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;
};

struct response_builder final : public tyrtech::message::struct_builder<0, 0>
{
    response_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }
};

struct response_parser final : public tyrtech::message::struct_parser<0, 0>
{
    response_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    response_parser() = default;
};

}

namespace messages::update {


struct request_builder final : public tyrtech::message::struct_builder<0, 24>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_id(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 0) = value;
    }

    void set_size(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 8) = value;
    }

    void set_key_count(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 16) = value;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<0, 24>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    decltype(auto) id() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 0);
    }

    decltype(auto) size() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 8);
    }

    decltype(auto) key_count() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 16);
    }
};

struct response_builder final : public tyrtech::message::struct_builder<0, 8>
{
    response_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_tid(uint64_t value)
    {
        *reinterpret_cast<uint64_t*>(m_static + 0) = value;
    }
};

struct response_parser final : public tyrtech::message::struct_parser<0, 8>
{
    response_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    response_parser() = default;

    decltype(auto) tid() const
    {
        return *reinterpret_cast<const uint64_t*>(m_static + 0);
    }
};

}

namespace messages::merge {


struct request_builder final : public tyrtech::message::struct_builder<0, 0>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }
};

struct request_parser final : public tyrtech::message::struct_parser<0, 0>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;
};

struct response_builder final : public tyrtech::message::struct_builder<1, 0>
{
    response_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_terminate(const uint8_t& value)
    {
        set_offset<0>();
        struct_builder<1, 0>::add_value(value);
    }

    static constexpr uint16_t terminate_bytes_required()
    {
        return tyrtech::message::element<uint8_t>::size;
    }
};

struct response_parser final : public tyrtech::message::struct_parser<1, 0>
{
    response_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    response_parser() = default;

    bool has_terminate() const
    {
        return has_offset<0>();
    }

    decltype(auto) terminate() const
    {
        return tyrtech::message::element<uint8_t>().parse(m_parser, offset<0>());
    }
};

}

DEFINE_SERVER_EXCEPTION(1, tyrtech::net::server_error_exception, invalid_request_exception);

constexpr void throw_module_exception(const tyrtech::net::service::error_parser& error)
{
    switch (error.code())
    {
        case -1:
        {
            throw tyrtech::net::unknown_module_exception("{}", error.message());
        }
        case -2:
        {
            throw tyrtech::net::unknown_function_exception("{}", error.message());
        }
        case 1:
        {
            throw invalid_request_exception("{}", error.message());
        }
        default:
        {
            throw tyrtech::net::unknown_exception("#{}: unknown exception", error.code());
        }
    }
}

static constexpr uint16_t id{1};

struct fetch
{
    static constexpr uint16_t id{1};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::fetch::request_builder;

    using request_parser_t =
            messages::fetch::request_parser;

    using response_builder_t =
            messages::fetch::response_builder;

    using response_parser_t =
            messages::fetch::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct update
{
    static constexpr uint16_t id{2};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::update::request_builder;

    using request_parser_t =
            messages::update::request_parser;

    using response_builder_t =
            messages::update::response_builder;

    using response_parser_t =
            messages::update::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct merge
{
    static constexpr uint16_t id{3};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::merge::request_builder;

    using request_parser_t =
            messages::merge::request_parser;

    using response_builder_t =
            messages::merge::response_builder;

    using response_parser_t =
            messages::merge::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

template<typename Implementation>
struct module : private tyrtech::disallow_copy
{
    Implementation* impl{nullptr};

    module(Implementation* impl)
      : impl(impl)
    {
    }

    void process_message(const tyrtech::net::service::request_parser& service_request,
                         tyrtech::net::socket_channel* channel,
                         typename Implementation::context* ctx)
    {
        switch (service_request.function())
        {
            case fetch::id:
            {
                using request_parser_t =
                        typename fetch::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->fetch(request, channel, ctx);

                break;
            }
            case update::id:
            {
                using request_parser_t =
                        typename update::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->update(request, channel, ctx);

                break;
            }
            case merge::id:
            {
                using request_parser_t =
                        typename merge::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->merge(request, channel, ctx);

                break;
            }
            default:
            {
                throw tyrtech::net::unknown_function_exception("#{}: unknown function", service_request.function());
            }
        }
    }

    decltype(auto) create_context(const std::shared_ptr<tyrtech::io::socket>& remote)
    {
        return impl->create_context(remote);
    }
};

}
