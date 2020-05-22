#pragma once


#include <net/server_exception.h>
#include <net/socket_channel.h>
#include <net/service.json.h>

#include <tyrdbs/meta_node/modules.json.h>


namespace tyrtech::tyrdbs::meta_node {


template<
    typename logImpl
>
struct service : private tyrtech::disallow_copy
{
    log::module<logImpl> log;

    service(
        logImpl* log
    )
      : log(log)
    {
    }

    struct context : private tyrtech::disallow_copy
    {
        typename logImpl::context log_ctx;

        context(
            typename logImpl::context&& log_ctx
        )
          : log_ctx(std::move(log_ctx))
        {
        }
    };

    context create_context(tyrtech::net::socket_channel* channel)
    {
        return context(
            log.create_context(channel)
        );
    }

    void process_message(const tyrtech::net::service::request_parser& service_request, context* ctx)
    {
        switch (service_request.module())
        {
            case log::id:
            {
                log.process_message(service_request, &ctx->log_ctx);

                break;
            }
            default:
            {
                throw tyrtech::net::unknown_module_exception("#{}: unknown module", service_request.function());
            }
        }
    }
};

}
