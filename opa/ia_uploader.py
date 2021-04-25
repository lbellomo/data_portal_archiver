import os
import json
import asyncio
import logging
from time import time
from pathlib import Path
from functools import partial
from concurrent.futures import ThreadPoolExecutor

import internetarchive
from requests import HTTPError

from utils import get_md5


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
            self.know_hashes = set(
                json.loads(line)["file_hash"] for line in self.p_internal_md.open()
            )
        else:
            self.know_hashes = set()

        self.pool = ThreadPoolExecutor(max_workers=count_workers)

    async def upload_resource(self, ia_id, ia_metadata, p_file, extra_md):
        loop = asyncio.get_running_loop()
        if not p_file.exists():
            logging.error(
                f"File {p_file} not exist! Resource: {extra_md['resource_name']}"
            )
            return
        file_hash = await loop.run_in_executor(self.pool, partial(get_md5, p_file))

        is_know_hash = await self.check_hash_in_internal_md(file_hash)
        if is_know_hash:  # file don't changed, skip (don't upload)
            logging.info(f"File {ia_id} have a know hash, skip.")
            p_file.unlink()  # remove unused local file
            return

        func_upload = partial(
            internetarchive.upload,
            ia_id,
            files=[str(p_file)],
            metadata=ia_metadata,
            access_key=self.ia_access_key,
            secret_key=self.ia_secret_key,
            retries=self.retries,
            retries_sleep=self.retries_sleep,
        )
        try:
            # _ = await loop.run_in_executor(self.pool, func_upload)
            pass
        except HTTPError as e:
            logging.error(
                f"Error {e} with file: "
                f"{extra_md['package_name']}, {extra_md['resource_name']}"
            )
            p_file.unlink()
            return

        logging.info(f"Uploaded {ia_id} to ia")
        p_file.unlink()  # remove local file after upload

        return {
            "ia_id": ia_id,
            "file_hash": file_hash,
            "timestamp": int(time()),
            "package_name": extra_md["package_name"],
            "resource_id": extra_md["resource_id"],
            "resource_name": extra_md["resource_name"],
        }

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
                    if (
                        count_end_signals := count_end_signals + 1
                    ) >= self.count_workers:
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

        logging.info(
            "New internal metadata writed, len with duplicates: "
            f"{len(all_md)}, len uniques: {len(uniques_md)}"
        )
