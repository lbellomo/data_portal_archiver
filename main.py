import os
import json
import asyncio
import logging
from time import time
from hashlib import md5
from pathlib import Path
from functools import partial
from urllib.parse import urljoin
from inspect import isasyncgenfunction
from concurrent.futures import ThreadPoolExecutor

# temp imports
# from pprint import pprint

import httpx
import internetarchive
from requests import HTTPError


class CkanCrawler:
    def __init__(self, base_url, portal_name):
        # TODO: try/validate the base url
        self.base_url = base_url
        # TODO: check valid portal_name (solo letras/numeros . - _)
        # Esto va a ser un nombre del dir tambien, que sea lindo sin espacios
        # y que arranque con una letra por las dudas
        self.portal_name = portal_name
        self.client = httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=10))

        self.p_base = Path(self.portal_name)
        self.p_items_md = self.p_base / "items_metadata.json"
        self.p_files = self.p_base / "files"
        self.p_metadata = self.p_base / "metadata"

        self.p_base.mkdir(exist_ok=True)
        self.p_files.mkdir(exist_ok=True)
        self.p_metadata.mkdir(exist_ok=True)

        self.url_package_list = urljoin(self.base_url, "/api/3/action/package_list")
        self.url_package_show = urljoin(self.base_url, "/api/3/action/package_show")

        # TODO: add valid formats from config file
        self.valid_formats = ["csv"]

    async def get_package_list(self):
        """Get a list of all packages ids"""
        try:
            r = await self.client.get(self.url_package_list)
            r.raise_for_status()
        except httpx.RequestError as exc:
            logging.error(f"An error occurred while requesting {exc.request.url!r}.")
        except httpx.HTTPStatusError as exc:
            logging.error(
                f"Error response {exc.response.status_code}"
                f" while requesting {exc.request.url!r}.")

        r_json = r.json()
        packages_list = r_json["result"]
        logging.info(f"Downloaded package list with {len(packages_list)} packages")
        return {"packages_list": packages_list}

    async def get_package_metadata(self, package_id):
        """Get the metadata from an package."""
        try:
            r = await self.client.get(self.url_package_show, params={"id": package_id})
            r.raise_for_status()
        except httpx.RequestError as exc:
            logging.error(f"An error occurred while requesting {exc.request.url!r}.")
        except httpx.HTTPStatusError as exc:
            logging.error(
                f"Error response {exc.response.status_code}"
                f" while requesting {exc.request.url!r}.")

        r_json = r.json()
        metadata = r_json["result"]

        logging.info(f"Downloaded metadata for package {package_id}")
        return {"metadata": metadata}

    async def process_package(self, metadata):
        save_metadata = False
        package_id = metadata["name"]
        p_package_md = self.p_metadata / f"{package_id}.json"

        # check for old md if any
        # if p_package_md.exists():  load

        # read the package metada and iter for all resources
        for resource in metadata["resources"]:
            # check valid format
            if resource["format"].lower() not in self.valid_formats:
                continue

            # TODO
            # check old metadata for that resource exist

            # for now save all the time
            save_metadata = True

            # TODO
            # check if was updated
            logging.info(f"Procesed resource {resource['name']} from package {metadata['name']}")
            yield {"resource": resource, "package_name": metadata["name"]}

        # if any resource change, write new metadata
        if save_metadata:
            # TODO: do with aiofiles
            with p_package_md.open("w") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2, sort_keys=True)
            logging.info(f"Saved metadata for resource {package_id}")

    async def download_resource(self, resource, package_name):
        """Save a resource to disk."""
        resource_url = resource["url"]
        extra_md = {"package_name": package_name,
                    "resource_id": resource["id"],
                    "resource_name": resource["name"]}

        # TODO: do with aiofiles
        p_package = self.p_files / package_name
        p_package.mkdir(exist_ok=True)
        p_file = p_package / resource_url.rsplit("/", maxsplit=1)[-1]

        try:
            with p_file.open("wb") as f:
                async with self.client.stream("GET", resource_url) as response:
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)
        except httpx.RequestError as e:
            logging.error(f"Error {e} downloading resource: {package_name} {resource['name']}")
            if p_file.exists():
                p_file.unlink()
            return


        ia_id, md = await self._create_ia_metadata(resource)
        logging.info(
            f"Downloaded file for resource {resource['name']}"
            f" from package {package_name}")
        return {"ia_id": ia_id, "ia_metadata": md, "p_file": p_file, "extra_md": extra_md}

    async def _create_ia_metadata(self, resource):
        description = resource["description"]

        # TODO: fix this (attributes have missing keys or another formats)
        # table = resource.get("attributesDescription")
        # if table:
        #     table = json.loads(table)
        #     pretty_table = [
        #         f"{attr['title']} [{attr['type']}]: {attr['description']}"
        #         for attr in table]

        #     description += "\n" + "\n - " + "\n - ".join(pretty_table)

        md = dict(
            title=resource["name"],
            description=description,
            mediatype="data"
        )

        ia_id = f"{self.portal_name}_{resource['id']}"
        if len(ia_id) >= 100:
            logging.warning(f"ia_id too long for resource {resource['name']} (max len == 100)")

        return ia_id, md


def get_md5(p_file: Path):
    """Calculate the md5 of a file"""
    with p_file.open("rb") as f:
        file_hash = md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()


class IaUploader:
    def __init__(self, portal_name, count_workers):
        self.portal_name = portal_name
        self.count_workers = count_workers
        # TODO: read from config file
        self.retries = 5
        self.retries_sleep = 60
        self.p_internal_md = Path(self.portal_name) / "internal_metadata.json"

        ia_access_key = os.environ.get("IA_ACCESS_KEY")
        ia_secret_key = os.environ.get("IA_SECRET_KEY")
        if not (ia_access_key and ia_secret_key):
            raise ValueError("Internet Archive acces_key or secret_key missing")

        self.ia_access_key = ia_access_key
        self.ia_secret_key = ia_secret_key

        if self.p_internal_md.exists():
            self.know_hashes = set(json.loads(line)["file_hash"] for line in self.p_internal_md.open())
        else:
            self.know_hashes = set()

        self.pool = ThreadPoolExecutor(max_workers=count_workers)

    async def upload_resource(self, ia_id, ia_metadata, p_file, extra_md):
        loop = asyncio.get_running_loop()

        file_hash = await loop.run_in_executor(self.pool, partial(get_md5, p_file))

        is_know_hash = await self.check_hash_in_internal_md(file_hash)
        if is_know_hash:  # file don't changed, skip (don't upload)
            p_file.unlink()  # remove unused local file
            return

        func_upload = partial(
            internetarchive.upload, ia_id, files=[str(p_file)], metadata=ia_metadata,
            access_key=self.ia_access_key, secret_key=self.ia_secret_key,
            retries=self.retries, retries_sleep=self.retries_sleep)
        try:
            _ = await loop.run_in_executor(self.pool, func_upload)
        except HTTPError as e:
            logging.error(
                f"Error {e} with file: {extra_md['package_name']}, {extra_md['resource_name']}")
            p_file.unlink()
            return

        logging.info(f"Uploaded {ia_id} to ia")
        p_file.unlink()  # remove local file after upload

        return {"ia_id": ia_id,
                "file_hash": file_hash,
                "timestamp": int(time()),
                "package_name": extra_md["package_name"],
                "resource_id": extra_md["resource_id"],
                "resource_name": extra_md["resource_name"]}

    async def check_hash_in_internal_md(self, file_hash):
        return file_hash in self.know_hashes

    async def write_internal_metadata(self, queue):
        # TODO: protect this coro to write even is there are any error
        count_end_signals = 0

        # Wait for all the workers to finish
        with self.p_internal_md.open("a") as f:
            while True:
                item = await queue.get()
                if not item:
                    if (count_end_signals := count_end_signals + 1) >= self.count_workers:
                        break

                    continue  # do nothing with the stop signal "None"

                # get all the not-None items
                # new_md.append(item)
                json.dump(item, f, ensure_ascii=False, sort_keys=True)
                f.write("\n")

        # Remove duplicated and keep only with the last
        with self.p_internal_md.open() as f:
            all_md = [json.loads(line) for line in f.readlines()]

        all_md.sort(key=lambda x: x["timestamp"], reverse=True)
        all_md.sort(key=lambda x: x["ia_id"])

        uniques_ia_ids = set()
        uniques_md = []
        for item in all_md:
            if item["ia_id"] not in uniques_ia_ids:
                uniques_md.append(item)
                uniques_ia_ids.add(item["ia_id"])

        with self.p_internal_md.open("w") as f:
            for item in uniques_md:
                json.dump(item, f, ensure_ascii=False, sort_keys=True)
                f.write("\n")

        # pprint(all_md)

        # # read old metadata
        # if self.p_internal_md.exists():
        #     old_md = json.load(self.p_internal_md.open())
        # else:
        #     old_md = []

        # # replace old metadata
        # new_ia_ids = set(item["ia_id"] for item in new_md)
        # # sort the items in a deterministic way for an easier diff
        # all_md = sorted(
        #     [md for md in old_md if md["ia_id"] not in new_ia_ids] + new_md,
        #     key=lambda k: k["ia_id"])

        # # save to file metadata
        # with self.p_internal_md.open("w") as f:
        #     json.dump(all_md, f, ensure_ascii=False, indent=2, sort_keys=True)

        # logging.info(f"New internal metadata, {len(new_md)} new items and {len(all_md)} in total")
        logging.info(f"New internal metadata writed, len with duplicates: {len(all_md)}, len uniques: {len(uniques_md)}")


async def create_worker(function, queue_in, queue_out=None):
    """Generic worker that process item from
    queue_in with function and put it in queue_out"""
    while True:
        item_in = await queue_in.get()
        # log get new item_in

        if not item_in:
            # no more items, put stop signal for next worker
            if queue_out:
                await queue_out.put(None)
            logging.info(f"No more work, close worker from {function.__name__}")
            break

        if isasyncgenfunction(function):
            async for item_out in function(**item_in):
                if item_out and queue_out:
                    await queue_out.put(item_out)

        else:
            item_out = await function(**item_in)
            if item_out and queue_out:
                await queue_out.put(item_out)

        # log new item_out
        # log put new item_out in queue_out


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

    # result["packages_list"] = ["subte-estaciones", "programa-aprende-programando"]  # debug

    for package in result["packages_list"]:
        queue_packages.put_nowait({"package_id": package})
    logging.info("queue_packages full with packages!")
    # only for test:
    # await queue_packages.put({"package_id": "subte-estaciones"})

    # add a stop signar for each worker
    for _ in range(count_workers):
        queue_packages.put_nowait(None)

    functions = [crawler.get_package_metadata,
                 crawler.process_package,
                 crawler.download_resource,
                 archiver.upload_resource]
    queues_in = [queue_packages, queue_metadata, queue_resources, queue_uploads]
    queues_out = [queue_metadata, queue_resources, queue_uploads, queue_internal_metadata]
    workers_names = ["package", "metadata", "resources", "upload"]
    # Start all the workers
    for func, queue_in, queue_out, workers_name in zip(functions, queues_in,
                                                       queues_out, workers_names):
        for _ in range(count_workers):
            worker = create_worker(func, queue_in, queue_out)
            workers[workers_name].append(asyncio.create_task(worker))

    # check if metadata is new

    # download item / discard item if not now

    # upload item to internet archive (ia)

    internal_md_tasks = asyncio.create_task(
        archiver.write_internal_metadata(queue_internal_metadata))

    # wait all the workers to finish
    all_tasks = [value for values in workers.values() for value in values]
    await asyncio.gather(*all_tasks)

    # wait the internal metadata
    await internal_md_tasks

    # cerramos el cliente
    await crawler.client.aclose()

    print("---\nEND MAIN\n---")

if __name__ == "__main__":
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    print("start")
    asyncio.run(main())
    print("end")
