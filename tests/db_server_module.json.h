#pragma once


#include <net/server_exception.h>
#include <net/socket_channel.h>
#include <net/service.json.h>


namespace tests::collections {


namespace messages::update_data {


struct request_builder final : public tyrtech::message::struct_builder<2, 0>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_handle(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<2, 0>::add_value(value);
    }

    static constexpr uint16_t handle_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }

    decltype(auto) add_data()
    {
        set_offset<1>();
        return m_builder;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<2, 0>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    bool has_handle() const
    {
        return has_offset<0>();
    }

    decltype(auto) handle() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
    }

    bool has_data() const
    {
        return has_offset<1>();
    }

    decltype(auto) data() const
    {
        return offset<1>();
    }
};

struct response_builder final : public tyrtech::message::struct_builder<1, 0>
{
    response_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_handle(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<1, 0>::add_value(value);
    }

    static constexpr uint16_t handle_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }
};

struct response_parser final : public tyrtech::message::struct_parser<1, 0>
{
    response_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    response_parser() = default;

    bool has_handle() const
    {
        return has_offset<0>();
    }

    decltype(auto) handle() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
    }
};

}

namespace messages::commit_update {


struct request_builder final : public tyrtech::message::struct_builder<1, 0>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_handle(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<1, 0>::add_value(value);
    }

    static constexpr uint16_t handle_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<1, 0>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    bool has_handle() const
    {
        return has_offset<0>();
    }

    decltype(auto) handle() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
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

namespace messages::rollback_update {


struct request_builder final : public tyrtech::message::struct_builder<1, 0>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_handle(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<1, 0>::add_value(value);
    }

    static constexpr uint16_t handle_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<1, 0>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    bool has_handle() const
    {
        return has_offset<0>();
    }

    decltype(auto) handle() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
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

namespace messages::fetch_data {


struct request_builder final : public tyrtech::message::struct_builder<4, 0>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_handle(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<4, 0>::add_value(value);
    }

    static constexpr uint16_t handle_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }

    void add_min_key(const std::string_view& value)
    {
        set_offset<1>();
        struct_builder<4, 0>::add_value(value);
    }

    static constexpr uint16_t min_key_bytes_required()
    {
        return tyrtech::message::element<std::string_view>::size;
    }

    void add_max_key(const std::string_view& value)
    {
        set_offset<2>();
        struct_builder<4, 0>::add_value(value);
    }

    static constexpr uint16_t max_key_bytes_required()
    {
        return tyrtech::message::element<std::string_view>::size;
    }

    void add_ushard(const uint32_t& value)
    {
        set_offset<3>();
        struct_builder<4, 0>::add_value(value);
    }

    static constexpr uint16_t ushard_bytes_required()
    {
        return tyrtech::message::element<uint32_t>::size;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<4, 0>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    bool has_handle() const
    {
        return has_offset<0>();
    }

    decltype(auto) handle() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
    }

    bool has_min_key() const
    {
        return has_offset<1>();
    }

    decltype(auto) min_key() const
    {
        return tyrtech::message::element<std::string_view>().parse(m_parser, offset<1>());
    }

    bool has_max_key() const
    {
        return has_offset<2>();
    }

    decltype(auto) max_key() const
    {
        return tyrtech::message::element<std::string_view>().parse(m_parser, offset<2>());
    }

    bool has_ushard() const
    {
        return has_offset<3>();
    }

    decltype(auto) ushard() const
    {
        return tyrtech::message::element<uint32_t>().parse(m_parser, offset<3>());
    }
};

struct response_builder final : public tyrtech::message::struct_builder<2, 0>
{
    response_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_handle(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<2, 0>::add_value(value);
    }

    static constexpr uint16_t handle_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }

    decltype(auto) add_data()
    {
        set_offset<1>();
        return m_builder;
    }
};

struct response_parser final : public tyrtech::message::struct_parser<2, 0>
{
    response_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    response_parser() = default;

    bool has_handle() const
    {
        return has_offset<0>();
    }

    decltype(auto) handle() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
    }

    bool has_data() const
    {
        return has_offset<1>();
    }

    decltype(auto) data() const
    {
        return offset<1>();
    }
};

}

namespace messages::abort_fetch {


struct request_builder final : public tyrtech::message::struct_builder<1, 0>
{
    request_builder(tyrtech::message::builder* builder)
      : struct_builder(builder)
    {
    }

    void add_handle(const uint64_t& value)
    {
        set_offset<0>();
        struct_builder<1, 0>::add_value(value);
    }

    static constexpr uint16_t handle_bytes_required()
    {
        return tyrtech::message::element<uint64_t>::size;
    }
};

struct request_parser final : public tyrtech::message::struct_parser<1, 0>
{
    request_parser(const tyrtech::message::parser* parser, uint16_t offset)
      : struct_parser(parser, offset)
    {
    }

    request_parser() = default;

    bool has_handle() const
    {
        return has_offset<0>();
    }

    decltype(auto) handle() const
    {
        return tyrtech::message::element<uint64_t>().parse(m_parser, offset<0>());
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
        default:
        {
            throw tyrtech::net::unknown_exception("#{}: unknown exception", error.code());
        }
    }
}

static constexpr uint16_t id{1};

struct update_data
{
    static constexpr uint16_t id{1};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::update_data::request_builder;

    using request_parser_t =
            messages::update_data::request_parser;

    using response_builder_t =
            messages::update_data::response_builder;

    using response_parser_t =
            messages::update_data::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct commit_update
{
    static constexpr uint16_t id{2};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::commit_update::request_builder;

    using request_parser_t =
            messages::commit_update::request_parser;

    using response_builder_t =
            messages::commit_update::response_builder;

    using response_parser_t =
            messages::commit_update::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct rollback_update
{
    static constexpr uint16_t id{3};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::rollback_update::request_builder;

    using request_parser_t =
            messages::rollback_update::request_parser;

    using response_builder_t =
            messages::rollback_update::response_builder;

    using response_parser_t =
            messages::rollback_update::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct fetch_data
{
    static constexpr uint16_t id{4};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::fetch_data::request_builder;

    using request_parser_t =
            messages::fetch_data::request_parser;

    using response_builder_t =
            messages::fetch_data::response_builder;

    using response_parser_t =
            messages::fetch_data::response_parser;

    static void throw_exception(const tyrtech::net::service::error_parser& error)
    {
        throw_module_exception(error);
    }
};

struct abort_fetch
{
    static constexpr uint16_t id{5};
    static constexpr uint16_t module_id{1};

    using request_builder_t =
            messages::abort_fetch::request_builder;

    using request_parser_t =
            messages::abort_fetch::request_parser;

    using response_builder_t =
            messages::abort_fetch::response_builder;

    using response_parser_t =
            messages::abort_fetch::response_parser;

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
                         typename Implementation::context* ctx)
    {
        switch (service_request.function())
        {
            case update_data::id:
            {
                using request_parser_t =
                        typename update_data::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->update_data(request, ctx);

                break;
            }
            case commit_update::id:
            {
                using request_parser_t =
                        typename commit_update::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->commit_update(request, ctx);

                break;
            }
            case rollback_update::id:
            {
                using request_parser_t =
                        typename rollback_update::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->rollback_update(request, ctx);

                break;
            }
            case fetch_data::id:
            {
                using request_parser_t =
                        typename fetch_data::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->fetch_data(request, ctx);

                break;
            }
            case abort_fetch::id:
            {
                using request_parser_t =
                        typename abort_fetch::request_parser_t;

                request_parser_t request(service_request.get_parser(),
                                         service_request.message());

                impl->abort_fetch(request, ctx);

                break;
            }
            default:
            {
                throw tyrtech::net::unknown_function_exception("#{}: unknown function", service_request.function());
            }
        }
    }

    decltype(auto) create_context(tyrtech::net::socket_channel* channel)
    {
        return impl->create_context(channel);
    }
};

}
