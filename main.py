import json
import asyncio
from pathlib import Path
from urllib.parse import urljoin
from inspect import isasyncgenfunction

# temp imports
from pprint import pprint

import httpx
from internetarchive import upload


class CkanCrawler:
    def __init__(self, base_url, portal_name):
        # TODO: try the base url
        self.base_url = base_url
        # TODO: check valid portal_name (solo letras/numeros . - _)
        self.portal_name = portal_name
        self.client = httpx.AsyncClient()

        # TODO: put this in a folder with the name of the portal
        self.p_files = Path("files")
        self.p_metadata = Path("metadata")

        self.p_files.mkdir(exist_ok=True)
        self.p_metadata.mkdir(exist_ok=True)

        self.url_package_list = urljoin(self.base_url, "/api/3/action/package_list")
        self.url_package_show = urljoin(self.base_url, "/api/3/action/package_show")

        # TODO: add valid formats from config file
        self.valid_formats = ["csv"]

    async def get_package_list(self):
        """Get a list of all packages ids"""
        print("bajando package list")
        r = await self.client.get(self.url_package_list)
        # TODO
        # assert 200
        # assert success == True in json
        r_json = r.json()
        packages_list = r_json["result"]
        return {"packages_list": packages_list}

    async def get_package_metadata(self, package_id, save=True):
        """Get the metadata from an package."""
        r = await self.client.get(self.url_package_show, params={"id": package_id})
        # TODO
        # assert 200
        # assert success == True in json
        r_json = r.json()
        metadata = r_json["result"]

        if save:
            # TODO: do with aiofiles
            with (self.p_metadata / f"{package_id}.json").open("w") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

        return {"metadata": metadata}

    async def process_package(self, metadata):
        # read the package metada and iter for all resources
        for resource in metadata["resources"]:
            print(f"processing package {metadata['name']}, {resource['name']}")

            # check valid format
            if resource["format"].lower() not in self.valid_formats:
                continue

            # TODO
            # check old metadata for that resource exist

            # TODO
            # check if was updated

            yield {"resource": resource, "package_name": metadata["name"]}

    async def download_upload_resource(self, resource, package_name):
        """Save a resource to disk."""
        # resource_id = resource["id"]
        resource_url = resource["url"]

        # TODO: do with aiofiles
        p_download = self.p_files / package_name
        p_download.mkdir(exist_ok=True)
        p_file = p_download / resource_url.rsplit("/", maxsplit=1)[-1]

        print(f"Downloading {p_file}, {resource_url}")

        # TODO: add retry (with transport)
        with p_file.open("wb") as f:
            async with self.client.stream("GET", resource_url) as response:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)

        # TODO: tirarlo en un async thread nuevo
        # TODO: asegurarse que sea menor a 100 caracteres (por lo menos tirar warning)
        print("subiendo a ia")
        description = resource["description"]

        table = resource.get("attributesDescription")
        if table:
            table = json.loads(table)
            pretty_table = [f"{attr['title']} [{attr['type']}]: {attr['description']}" for attr in table]
            description += "\n"

            description += "\n - " + "\n - ".join(pretty_table)

        md = dict(
            title=resource["name"],
            description=description,
            mediatype="data"
        )

        ia_id = f"{self.portal_name}_{resource['id']}"

        r = upload(ia_id, files=[str(p_file)], metadata=md)
        print(f"upload a ai: {r[0].status_code}")
        # TODO: escribir archivo final con la metadata (id archivo, md5, id ai, tal vez resource-name y package-name)
        # TODO: mover la logica de ia afuera del scraper
        # TODO: borrar archivo una vez bajado


async def create_worker(function, queue_in, queue_out=None):
    """Generic worker that process item from
    queue_in with function and put it in queue_out"""
    while True:
        item_in = await queue_in.get()
        print(f"in: {function.__name__}")
        print(f"{item_in=}")
        # log get new item_in

        if not item_in:
            print("ultimo item! hora de matar al workier!")
            # no more items, put stop signal for next worker
            if queue_out:
                await queue_out.put(None)

            break

        print(f"{function.__name__}, {isasyncgenfunction(function)=}")
        if isasyncgenfunction(function):
            print("is generator ->", function.__name__)
            async for item_out in function(**item_in):
                if queue_out:
                    await queue_out.put(item_out)

        else:
            print("normal function ->", function.__name__)
            item_out = await function(**item_in)
            if queue_out:
                await queue_out.put(item_out)

        # log new item_out
        # log put new item_out in queue_out


async def main():
    # read config

    # total nomber of workers for each task
    count_workers = 5
    maxsize = 5

    workers = {"package": [], "metadata": [], "resources": []}

    # start queues
    queue_packages = asyncio.Queue()
    queue_metadata = asyncio.Queue(maxsize=maxsize)
    queue_resources = asyncio.Queue(maxsize=maxsize)

    base_url = "https://data.buenosaires.gob.ar/"
    portal_name = "buenos_aires_data"
    crawler = CkanCrawler(base_url, portal_name)

    # download all metada from portal
    # an put metadata in the first queue
    # TODO: hacerlo async a esta parte, sin llenar esta queue primero
    result = await crawler.get_package_list()
    print("poniendo paquetes en 'queue_packages'")
    for package in result["packages_list"]:
        await queue_packages.put({"package_id": package})
    print("terminado de poner paquetes iniciales en 'queue_packages")

    # add a stop signar for each worker
    for _ in range(count_workers):
        await queue_packages.put(None)

    functions = [crawler.get_package_metadata,
                 crawler.process_package,
                 crawler.download_upload_resource]
    queues_in = [queue_packages, queue_metadata, queue_resources]
    queues_out = [queue_metadata, queue_resources, None]
    workers_names = ["package", "metadata", "resources"]
    # Start all the workers
    for func, queue_in, queue_out, workers_name in zip(functions, queues_in,
                                                       queues_out, workers_names):
        for _ in range(count_workers):
            worker = create_worker(func, queue_in, queue_out)
            workers[workers_name].append(asyncio.create_task(worker))

    # check if metadata is new

    # download item / discard item if not now

    # upload item to internet archive (ia)

    # save new metadata
    all_taks = workers["package"] + workers["metadata"] + workers["resources"]
    await asyncio.gather(*all_taks)

    # cerramos el cliente
    await crawler.client.aclose()

    print("---\nEND MAIN\n---")


async def dev_main():
    base_url = "https://data.buenosaires.gob.ar/"
    portal_name = "buenos_aires_data"
    crawler = CkanCrawler(base_url, portal_name)

    result = await crawler.get_package_list()
    r = result["packages_list"]
    print(f"{len(r)=}, {r[10]=}")
    package_id = r[10]  # actuaciones-fiscalizacion
    package_id = "bocas-subte"
    metadata = await crawler.get_package_metadata(package_id)
    metadata = metadata["metadata"]
    pprint(metadata)

    print("raw metadata:")
    print(f"{metadata!r}")
    package_name = metadata["name"]
    resource = metadata["resources"][0]
    await crawler.download_upload_resource(resource, package_name)

    # cerramos el cliente
    await crawler.client.aclose()

if __name__ == "__main__":
    print("start")
    asyncio.run(dev_main())
    print("end")
