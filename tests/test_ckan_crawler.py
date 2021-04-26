import pytest

from opa.ckan_crawler import CkanCrawler


# URL = 'https://base_url.example'
PACKAGE_LIST = ["example_a", "example_b", "example_c"]
PACKAGE_LIST_JSON = {"result": PACKAGE_LIST}


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


# @pytest.mark.asyncio
# async def test_request_error_get_package_list(tmp_path, httpx_mock):
#     pass


# @pytest.mark.asyncio
# async def test_request_status_get_package_list(tmp_path, httpx_mock):
#     pass


# get_package_metadata

# download_resource