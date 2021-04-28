import logging
from hashlib import md5
from pathlib import Path
from inspect import isasyncgenfunction


def get_md5(p_file: Path):
    """Calculate the md5 of a file"""
    with p_file.open("rb") as f:
        file_hash = md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()


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
