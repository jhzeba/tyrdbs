#include <common/cpu_sched.h>
#include <gt/engine.h>
#include <io/engine.h>
#include <io/uri.h>
#include <net/rpc_server.h>
#include <net/rpc_request.h>

#include <tests/services.json.h>


using namespace tyrtech;


namespace module1 {


using namespace tests::module1;


struct impl : private disallow_copy
{
    struct context : private disallow_copy
    {
    };

    context create_context(const std::shared_ptr<io::socket>& remote)
    {
        return context();
    }

    void func1(const func1::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx)
    {
        logger::debug("module1::func1 request: {} {}", request.param1(), request.param2());

        net::rpc_response<tests::module1::func1> response(channel);
        auto message = response.add_message();

        message.add_param1(request.param1());
        message.add_param2(request.param2());

        response.send();
    }

    void func2(const func2::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx)
    {
        logger::debug("module1::func2 request: {} {}", request.param1(), request.param2());

        net::rpc_response<tests::module1::func2> response(channel);
        auto message = response.add_message();

        message.add_param1(request.param1());
        message.add_param2(request.param2());

        response.send();
    }
};

}

namespace module2 {


using namespace tests::module2;


struct impl : private disallow_copy
{
    struct context : private disallow_copy
    {
    };

    context create_context(const std::shared_ptr<io::socket>& remote)
    {
        return context();
    }

    void func1(const func1::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx)
    {
        logger::debug("module2::func1 request: {} {}", request.param1(), request.param2());

        net::rpc_response<tests::module2::func1> response(channel);
        auto message = response.add_message();

        message.add_param1(request.param1());
        message.add_param2(request.param2());

        response.send();
    }

    void func2(const func2::request_parser_t& request,
               net::socket_channel* channel,
               context* ctx)
    {
        logger::debug("module2::func2 request: {} {}", request.param1(), request.param2());

        net::rpc_response<tests::module2::func2> response(channel);
        auto message = response.add_message();

        message.add_param1(request.param1());
        message.add_param2(request.param2());

        response.send();
    }
};

}

using service1_t =
        tests::service1<module1::impl, module2::impl>;

using server_t =
        net::rpc_server<service1_t>;


void client(server_t* s)
{
    for (uint32_t i = 0; i < 1000; i++)
    {
        logger::debug("iteration: {}", i);

        net::socket_channel channel(io::uri::connect(s->uri(), 0), 0);

        {
            net::rpc_request<tests::module1::func1> request(&channel);
            auto message = request.add_message();

            message.add_param1(1);
            message.add_param2("test1");

            request.execute();
            auto response = request.wait();

            logger::debug("module1::func1 response: {} {}", response.param1(), response.param2());
        }

        {
            net::rpc_request<tests::module2::func2> request(&channel);
            auto message = request.add_message();

            message.add_param1(2);
            message.add_param2("test2");

            request.execute();
            auto response = request.wait();

            logger::debug("module2::func2 response: {} {}", response.param1(), response.param2());
        }
    }

    s->terminate();
}


int main()
{
    set_cpu(0);

    gt::initialize();
    io::initialize(4096);
    io::socket::initialize(64);

    logger::set(logger::level::debug);

    //std::string_view uri("tcp://localhost:1234");
    std::string_view uri("unix://@/test.sock");

    module1::impl m1;
    module2::impl m2;

    service1_t srv(&m1, &m2);

    server_t s(io::uri::listen(uri), &srv);

    gt::create_thread(&client, &s);

    gt::run();

    return 0;
}
