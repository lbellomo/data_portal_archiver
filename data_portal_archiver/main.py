import os
import sys
import asyncio
import logging
from pathlib import Path

import toml

from data_portal_archiver.ckan_crawler import CkanCrawler
from data_portal_archiver.ia_uploader import IaUploader
from data_portal_archiver.utils import create_worker, write_portal_readme

# temp imports
# from pprint import pprint

default_count_workers = 5
default_maxsize = 5
default_save_package_metadata = True
default_upload_resources = True
default_save_internal_metadata = True


async def main(section_name):
    # read config
    # TODO: move to another funcion
    # TODO: made a config object (maybe pydantic) and pass the object
    p_config = Path("portals.toml")
    config = toml.load(p_config.open())
    general_config = config.get("global", dict())
    sections_config = config.get("section", dict())

    count_workers = general_config.get("count_workers", default_count_workers)
    maxsize = general_config.get("maxsize", default_maxsize)
    save_package_metadata = general_config.get(
        "save_package_metadata", default_save_package_metadata
    )
    upload_resources = general_config.get("upload_resources", default_upload_resources)
    save_internal_metadata = general_config.get(
        "save_internal_metadata", default_save_internal_metadata
    )

    if not sections_config:
        logging.error("Missing 'section' field in config!")
        sys.exist(1)

    if section_name not in sections_config:
        logging.error(f"Section {section_name} not in config!")
        sys.exist(1)

    section_config = sections_config[section_name]

    if not all(k in section_config for k in ["base_url", "portal_name"]):
        logging.error(f"Section {section_name} missing 'base_url' or 'portal_name'!")
        sys.exist(1)

    base_url = section_config["base_url"]
    portal_name = section_config["portal_name"]

    workers = {"package": [], "metadata": [], "resources": [], "upload": []}

    # start queues
    queue_packages = asyncio.Queue()
    queue_metadata = asyncio.Queue(maxsize=maxsize)
    queue_resources = asyncio.Queue(maxsize=maxsize)
    queue_uploads = asyncio.Queue(maxsize=maxsize)
    queue_internal_metadata = asyncio.Queue(maxsize=maxsize)

    crawler = CkanCrawler(base_url, portal_name, save_package_metadata)
    archiver = IaUploader(
        portal_name, count_workers, upload_resources, save_internal_metadata
    )

    # download all metada from portal
    # an put metadata in the first queue
    # TODO: hacerlo async a esta parte, sin llenar esta queue primero
    r_package_list = await crawler.get_package_list()

    # TODO: mover esta parte de allow packages a la config
    r_package_list["packages_list"] = [
        "subte-estaciones",
        "programa-aprende-programando",
    ]  # debug

    for package in r_package_list["packages_list"]:
        queue_packages.put_nowait({"package_id": package})
    logging.info("queue_packages full with packages!")
    # only for test:
    # await queue_packages.put({"package_id": "subte-estaciones"})

    # add a stop signar for each worker
    for _ in range(count_workers):
        queue_packages.put_nowait(None)

    functions = [
        crawler.get_package_metadata,
        crawler.process_package,
        crawler.download_resource,
        archiver.upload_resource,
    ]
    queues_in = [queue_packages, queue_metadata, queue_resources, queue_uploads]
    queues_out = [
        queue_metadata,
        queue_resources,
        queue_uploads,
        queue_internal_metadata,
    ]
    workers_names = ["package", "metadata", "resources", "upload"]
    # Start all the workers
    for func, queue_in, queue_out, workers_name in zip(
        functions, queues_in, queues_out, workers_names
    ):
        for _ in range(count_workers):
            worker = create_worker(func, queue_in, queue_out)
            workers[workers_name].append(asyncio.create_task(worker))

    # check if metadata is new

    # download item / discard item if not now

    # upload item to internet archive (ia)

    internal_md_tasks = asyncio.create_task(
        archiver.write_internal_metadata(queue_internal_metadata)
    )

    # wait all the workers to finish
    all_tasks = [value for values in workers.values() for value in values]
    await asyncio.gather(*all_tasks)

    # wait the internal metadata
    await internal_md_tasks

    # write final portal readme
    write_portal_readme(portal_name, base_url)
    logging.info("Portal readme updated")

    # cerramos el cliente
    await crawler.client.aclose()

    print("---\nEND MAIN\n---")


def run():
    args = sys.argv[1:]
    env_section_name = os.environ.get("SECTION_NAME")

    if not args and not env_section_name:
        logging.error("You need to pass the section_name like: 'dpa some_section_name'")
        sys.exit(1)
    elif not args:
        section_name = env_section_name
    else:
        section_name = args[0]

    logging.basicConfig(encoding="utf-8", level=logging.INFO)
    asyncio.run(main(section_name=section_name))
