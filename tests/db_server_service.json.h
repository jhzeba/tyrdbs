#pragma once


#include <net/server_exception.h>
#include <net/socket_channel.h>
#include <net/service.json.h>

#include <tests/db_server_module.json.h>


namespace tests {


template<
    typename collectionsImpl
>
struct db_server_service : private tyrtech::disallow_copy
{
    collections::module<collectionsImpl> collections;

    db_server_service(
        collectionsImpl* collections
    )
      : collections(collections)
    {
    }

    struct context : private tyrtech::disallow_copy
    {
        typename collectionsImpl::context collections_ctx;

        context(
            typename collectionsImpl::context&& collections_ctx
        )
          : collections_ctx(std::move(collections_ctx))
        {
        }
    };

    context create_context(tyrtech::net::socket_channel* channel)
    {
        return context(
            collections.create_context(channel)
        );
    }

    void process_message(const tyrtech::net::service::request_parser& service_request, context* ctx)
    {
        switch (service_request.module())
        {
            case collections::id:
            {
                collections.process_message(service_request, &ctx->collections_ctx);

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
