#include <tyrdbs/slice_writer.h>
#include <tyrdbs/overwrite_iterator.h>

#include <cassert>

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>


using namespace tyrtech;


class string_writer : public tyrdbs::writer
{
public:
    uint32_t write(const char* data, uint32_t size) override
    {
        m_buffer->append(data, size);
        return size;
    }

    uint64_t offset() const override
    {
        return m_buffer->size();
    }

    void flush() override
    {
    }

    void unlink() override
    {
    }

public:
    string_writer(std::string* buffer)
      : m_buffer(buffer)
    {
    }

private:
    std::string* m_buffer{nullptr};
};


class string_reader : public tyrdbs::reader
{
public:
    uint32_t pread(uint64_t offset, char* data, uint32_t size) const override
    {
        assert(offset + size <= m_buffer.size());
        memcpy(data, m_buffer.data() + offset, size);

        return size;
    }

    void unlink() override
    {
    }

public:
    string_reader(const std::string_view& buffer)
      : m_buffer(buffer)
    {
    }

private:
    std::string_view m_buffer;
};


std::string get_value_for(tyrdbs::slice* slice, const std::string_view& key)
{
    auto it = slice->range(key, key);

    CHECK(it != nullptr);
    CHECK(it->next() == true);

    std::string value;

    value.append(it->value());

    while (it->eor() == false)
    {
        CHECK(it->next() == true);
        value.append(it->value());
    }

    return value;
}


TEST_CASE("split_key_1")
{
    std::string data;

    {
        auto sw = std::make_shared<string_writer>(&data);
        auto slice = tyrdbs::slice_writer(std::move(sw));

        {
            std::array<char, 8109> value;
            value.fill(0x01);

            slice.add("0001", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x01);

            slice.add("0002", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x01);

            slice.add("0003", std::string_view(value.data(), value.size()), true, false);
        }

        slice.flush();
        slice.commit();
    }

    {
        auto sr = std::make_shared<string_reader>(data);
        auto slice = tyrdbs::slice(data.size(), std::move(sr));

        slice.set_cache_id(1);

        {
            auto value = get_value_for(&slice, "0001");
            CHECK(value.size() == 8109);
        }

        {
            auto value = get_value_for(&slice, "0002");
            CHECK(value.size() == 4096);
        }

        {
            auto value = get_value_for(&slice, "0003");
            CHECK(value.size() == 4096);
        }
    }
}


TEST_CASE("split_key_2")
{
    std::string data;

    {
        auto sw = std::make_shared<string_writer>(&data);
        auto slice = tyrdbs::slice_writer(std::move(sw));

        {
            std::array<char, 4096> value;
            value.fill(0x01);

            slice.add("0001", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 8192> value;
            value.fill(0x02);

            slice.add("0002", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x03);

            slice.add("0003", std::string_view(value.data(), value.size()), true, false);
        }

        slice.flush();
        slice.commit();
    }

    {
        auto sr = std::make_shared<string_reader>(data);
        auto slice = tyrdbs::slice(data.size(), std::move(sr));

        slice.set_cache_id(1);

        {
            auto value = get_value_for(&slice, "0001");
            CHECK(value.size() == 4096);
        }

        {
            auto value = get_value_for(&slice, "0002");
            CHECK(value.size() == 8192);
        }

        {
            auto value = get_value_for(&slice, "0003");
            CHECK(value.size() == 4096);
        }
    }
}

TEST_CASE("split_key_3")
{
    std::string data;

    {
        auto sw = std::make_shared<string_writer>(&data);
        auto slice = tyrdbs::slice_writer(std::move(sw));

        {
            std::array<char, 8108> value;
            value.fill(0x01);

            slice.add("0001", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x02);

            slice.add("0002", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x03);

            slice.add("0003", std::string_view(value.data(), value.size()), true, false);
        }

        slice.flush();
        slice.commit();
    }

    {
        auto sr = std::make_shared<string_reader>(data);
        auto slice = tyrdbs::slice(data.size(), std::move(sr));

        slice.set_cache_id(1);

        {
            auto value = get_value_for(&slice, "0001");
            CHECK(value.size() == 8108);
        }

        {
            auto value = get_value_for(&slice, "0002");
            CHECK(value.size() == 4096);
        }

        {
            auto value = get_value_for(&slice, "0003");
            CHECK(value.size() == 4096);
        }
    }
}

TEST_CASE("split_key_4")
{
    std::string data;

    {
        auto sw = std::make_shared<string_writer>(&data);
        auto slice = tyrdbs::slice_writer(std::move(sw));

        {
            std::array<char, 8108> value;
            value.fill(0x01);

            slice.add("0001", std::string_view(value.data(), value.size()), false, false);
        }

        {
            std::array<char, 1> value;
            value.fill(0x01);

            slice.add("0001", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x02);

            slice.add("0002", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x03);

            slice.add("0003", std::string_view(value.data(), value.size()), true, false);
        }

        slice.flush();
        slice.commit();
    }

    {
        auto sr = std::make_shared<string_reader>(data);
        auto slice = tyrdbs::slice(data.size(), std::move(sr));

        slice.set_cache_id(1);

        {
            auto value = get_value_for(&slice, "0001");
            CHECK(value.size() == 8109);
        }

        {
            auto value = get_value_for(&slice, "0002");
            CHECK(value.size() == 4096);
        }

        {
            auto value = get_value_for(&slice, "0003");
            CHECK(value.size() == 4096);
        }
    }
}

TEST_CASE("split_key_5")
{
    std::string data;

    {
        auto sw = std::make_shared<string_writer>(&data);
        auto slice = tyrdbs::slice_writer(std::move(sw));

        {
            std::array<char, 4096> value;
            value.fill(0x01);

            slice.add("0001", std::string_view(value.data(), value.size()), false, false);
        }

        {
            std::array<char, 8192> value;
            value.fill(0x01);

            slice.add("0001", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x02);

            slice.add("0002", std::string_view(value.data(), value.size()), true, false);
        }

        {
            std::array<char, 4096> value;
            value.fill(0x03);

            slice.add("0003", std::string_view(value.data(), value.size()), true, false);
        }

        slice.flush();
        slice.commit();
    }

    {
        auto sr = std::make_shared<string_reader>(data);
        auto slice = tyrdbs::slice(data.size(), std::move(sr));

        slice.set_cache_id(1);

        {
            auto value = get_value_for(&slice, "0001");
            CHECK(value.size() == 12288);
        }

        {
            auto value = get_value_for(&slice, "0002");
            CHECK(value.size() == 4096);
        }

        {
            auto value = get_value_for(&slice, "0003");
            CHECK(value.size() == 4096);
        }
    }
}
