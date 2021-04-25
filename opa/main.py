import asyncio
import logging

from ckan_crawler import CkanCrawler
from ia_uploader import IaUploader
from utils import create_worker

# temp imports
# from pprint import pprint


async def main():
    # read config

    # total nomber of workers for each task
    count_workers = 5
    maxsize = 5

    workers = {"package": [], "metadata": [], "resources": [], "upload": []}

    # start queues
    queue_packages = asyncio.Queue()
    queue_metadata = asyncio.Queue(maxsize=maxsize)
    queue_resources = asyncio.Queue(maxsize=maxsize)
    queue_uploads = asyncio.Queue(maxsize=maxsize)
    queue_internal_metadata = asyncio.Queue(maxsize=maxsize)

    base_url = "https://data.buenosaires.gob.ar/"
    portal_name = "buenos_aires_data"
    crawler = CkanCrawler(base_url, portal_name)
    archiver = IaUploader(portal_name, count_workers)

    # download all metada from portal
    # an put metadata in the first queue
    # TODO: hacerlo async a esta parte, sin llenar esta queue primero
    result = await crawler.get_package_list()

    result["packages_list"] = ["subte-estaciones",
                               "programa-aprende-programando"]  # debug

    for package in result["packages_list"]:
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

    # cerramos el cliente
    await crawler.client.aclose()

    print("---\nEND MAIN\n---")


if __name__ == "__main__":
    logging.basicConfig(encoding="utf-8", level=logging.INFO)
    print("start")
    asyncio.run(main())
    print("end")
