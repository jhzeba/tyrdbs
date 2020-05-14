#include <gt/engine.h>
#include <io/file.h>
#include <io/uri.h>
#include <net/rpc_server.h>

#include <tyrdbs/meta_node/service.h>
#include <tyrdbs/meta_node/service.json.h>
#include <tyrdbs/meta_node/log_module.h>


namespace tyrtech::tyrdbs::meta_node {


using service_t =
        service<log::impl>;

using server_t =
        net::rpc_server<8192, service_t>;


void service_thread(uint32_t ushards)
{
    auto ch = io::uri::listen("tcp://localhost:1234");

    log::impl impl("data", ushards);
    service_t service(&impl);

    server_t server(ch, &service);

    while (gt::terminated() == false)
    {
        gt::sleep(100);
    }
}

}
