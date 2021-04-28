import pytest

from ckan_crawler import CkanCrawler


# URL = 'https://base_url.example'
PACKAGE_LIST = ["example_a", "example_b", "example_c"]
PACKAGE_LIST_JSON = {"result": PACKAGE_LIST}
PACKAGE_ID = "example_a"
PACKAGE_METADATA = {
    "author": "some one",
    "id": "abc123",
    "name": "spam name",
    "resources": [],
}
PACKAGE_METADATA_JSON = {"result": PACKAGE_METADATA}


def make_crawler(tmp_path):
    crawler = CkanCrawler("http://base_url.example", "example_portal_name", tmp_path)
    return crawler


# TODO: run this in a fixture in an automatic way
async def close_crawler_client(crawler):
    await crawler.client.aclose()


@pytest.fixture
def assert_all_responses_were_requested() -> bool:
    return False


@pytest.mark.asyncio
async def test_init(tmp_path, httpx_mock):

    crawler = make_crawler(tmp_path)
    assert crawler.p_base.is_dir()
    assert crawler.p_files.is_dir()
    assert crawler.p_metadata.is_dir()

    await close_crawler_client(crawler)


@pytest.mark.asyncio
async def test_get_package_list(tmp_path, httpx_mock):
    httpx_mock.add_response(json=PACKAGE_LIST_JSON)

    crawler = make_crawler(tmp_path)
    r_package_list = await crawler.get_package_list()
    assert r_package_list == {"packages_list": PACKAGE_LIST}

    await close_crawler_client(crawler)


@pytest.mark.asyncio
async def test_get_package_metadata(tmp_path, httpx_mock):
    httpx_mock.add_response(json=PACKAGE_METADATA_JSON)

    crawler = make_crawler(tmp_path)
    r_package_list = await crawler.get_package_metadata(PACKAGE_ID)
    assert r_package_list == {"metadata": PACKAGE_METADATA}

    await close_crawler_client(crawler)


# @pytest.mark.asyncio
# async def test_process_package(tmp_path):
#     crawler = make_crawler(tmp_path)

#     await close_crawler_client(crawler)

# download_resource

# _create_ia_metadata
