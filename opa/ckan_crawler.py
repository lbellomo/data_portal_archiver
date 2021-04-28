import json
import logging
from pathlib import Path
from urllib.parse import urljoin

import httpx


class CkanCrawler:
    def __init__(
        self,
        base_url,
        portal_name,
        save_metadata,
        base_path=Path(),
    ):
        # TODO: try/validate the base url
        self.base_url = base_url
        # TODO: check valid portal_name (solo letras/numeros . - _)
        # Esto va a ser un nombre del dir tambien, que sea lindo sin espacios
        # y que arranque con una letra por las dudas
        self.portal_name = portal_name
        self.save_metadata = save_metadata
        self.client = httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(retries=10))

        self.p_base = base_path / self.portal_name
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
            return
        except httpx.HTTPStatusError as exc:
            logging.error(
                f"Error response {exc.response.status_code}"
                f" while requesting {exc.request.url!r}."
            )
            return

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
                f" while requesting {exc.request.url!r}."
            )

        r_json = r.json()
        metadata = r_json["result"]

        logging.info(f"Downloaded metadata for package {package_id}")
        return {"metadata": metadata}

    async def process_package(self, metadata):
        is_new = False
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
            is_new = True

            # TODO
            # check if was updated
            logging.info(
                f"Procesed resource {resource['name']} from package {metadata['name']}"
            )
            yield {"resource": resource, "package_name": metadata["name"]}

        # if any resource change, write new metadata
        if self.save_metadata and is_new:
            # TODO: do with aiofiles
            with p_package_md.open("w") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2, sort_keys=True)
            logging.info(f"Saved metadata for resource {package_id}")

    async def download_resource(self, resource, package_name):
        """Save a resource to disk."""
        resource_url = resource["url"]
        extra_md = {
            "package_name": package_name,
            "resource_id": resource["id"],
            "resource_name": resource["name"],
        }

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
            logging.error(
                f"Error {e} downloading resource: {package_name} {resource['name']}"
            )
            if p_file.exists():
                p_file.unlink()
            return

        ia_id, md = await self._create_ia_metadata(resource)
        logging.info(
            f"Downloaded file for resource {resource['name']}"
            f" from package {package_name}"
        )
        return {
            "ia_id": ia_id,
            "ia_metadata": md,
            "p_file": p_file,
            "extra_md": extra_md,
        }

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

        md = dict(title=resource["name"], description=description, mediatype="data")

        ia_id = f"{self.portal_name}_{resource['id']}"
        if len(ia_id) >= 100:
            logging.warning(
                f"ia_id too long for resource {resource['name']} (max len == 100)"
            )

        return ia_id, md
