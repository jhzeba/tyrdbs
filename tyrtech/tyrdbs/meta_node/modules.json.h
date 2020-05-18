#pragma once


#include <net/socket_channel.h>
#include <net/server_exception.h>
#include <net/service.json.h>


namespace tyrtech::tyrdbs::meta_node::log {


namespace messages::fetch_slices {


struct request_builder final : public tyrtech::message::struct_builder<0, 2>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_ushard_id(uint16_t value)
    {
        *reinterpret_cast<uint16_t*>(m_static + 0) = value;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<0, 2>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    decltype(auto) ushard_id() const
    {
        return *reinterpret_cast<const uint16_t*>(m_static + 0);
    }
};

struct response_builder final : public tyrtech::message::struct_builder<0, 4>
{
    response_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_handle(uint32_t value)
    {
        *reinterpret_cast<uint32_t*>(m_static + 0) = value;
    }
};

struct response_parser final : public tyrtech::message::struct_parser<0, 4>
{
    response_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    response_parser() = default;

    decltype(auto) handle() const
    {
        return *reinterpret_cast<const uint32_t*>(m_static + 0);
    }
};

}

namespace messages::release_slices {


struct request_builder final : public tyrtech::message::struct_builder<0, 4>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_handle(uint32_t value)
    {
        *reinterpret_cast<uint32_t*>(m_static + 0) = value;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<0, 4>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    decltype(auto) handle() const
    {
        return *reinterpret_cast<const uint32_t*>(m_static + 0);
    }
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

namespace messages::update_slices {


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

namespace messages::fetch_slices_to_merge {


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

struct response_builder final : public tyrtech::message::struct_builder<0, 2>
{
    response_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_ushard_id(uint16_t value)
    {
        *reinterpret_cast<uint16_t*>(m_static + 0) = value;
    }
};

struct response_parser final : public tyrtech::message::struct_parser<0, 2>
{
    response_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    response_parser() = default;

    decltype(auto) ushard_id() const
    {
        return *reinterpret_cast<const uint16_t*>(m_static + 0);
    }
};

}

namespace messages::merge_slices {


struct request_builder final : public tyrtech::message::struct_builder<0, 2>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void set_ushard_id(uint16_t value)
    {
        *reinterpret_cast<uint16_t*>(m_static + 0) = value;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<0, 2>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    decltype(auto) ushard_id() const
    {
        return *reinterpret_cast<const uint16_t*>(m_static + 0);
    }
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

struct fetch_slices
{
    static constexpr uint16_t id{1};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::fetch_slices::request_builder;

    using request_parser_t =
            messages::fetch_slices::request_parser;

    using response_builder_t =
            messages::fetch_slices::response_builder;

    using response_parser_t =
            messages::fetch_slices::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct release_slices
{
    static constexpr uint16_t id{2};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::release_slices::request_builder;

    using request_parser_t =
            messages::release_slices::request_parser;

    using response_builder_t =
            messages::release_slices::response_builder;

    using response_parser_t =
            messages::release_slices::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct update_slices
{
    static constexpr uint16_t id{3};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::update_slices::request_builder;

    using request_parser_t =
            messages::update_slices::request_parser;

    using response_builder_t =
            messages::update_slices::response_builder;

    using response_parser_t =
            messages::update_slices::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct fetch_slices_to_merge
{
    static constexpr uint16_t id{4};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::fetch_slices_to_merge::request_builder;

    using request_parser_t =
            messages::fetch_slices_to_merge::request_parser;

    using response_builder_t =
            messages::fetch_slices_to_merge::response_builder;

    using response_parser_t =
            messages::fetch_slices_to_merge::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct merge_slices
{
    static constexpr uint16_t id{5};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::merge_slices::request_builder;

    using request_parser_t =
            messages::merge_slices::request_parser;

    using response_builder_t =
            messages::merge_slices::response_builder;

    using response_parser_t =
            messages::merge_slices::response_parser;

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
            case fetch_slices::id:
            {
                using request_parser_t =
                        typename fetch_slices::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->fetch_slices(request, channel, ctx);

                break;
            }
            case release_slices::id:
            {
                using request_parser_t =
                        typename release_slices::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->release_slices(request, channel, ctx);

                break;
            }
            case update_slices::id:
            {
                using request_parser_t =
                        typename update_slices::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->update_slices(request, channel, ctx);

                break;
            }
            case fetch_slices_to_merge::id:
            {
                using request_parser_t =
                        typename fetch_slices_to_merge::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->fetch_slices_to_merge(request, channel, ctx);

                break;
            }
            case merge_slices::id:
            {
                using request_parser_t =
                        typename merge_slices::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->merge_slices(request, channel, ctx);

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
