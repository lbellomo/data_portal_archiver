import json
import logging
from hashlib import md5
from pathlib import Path
from urllib.parse import urljoin
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


# write markdown utils
def get_package_json(p_md, package_name):
    p_package_md = p_md / f"{package_name}.json"
    if not p_package_md.exists():
        return {}
    with p_package_md.open() as f:
        return json.load(f)


def write_package_intro(item_package, item_package_json):
    title = item_package_json.get("title", "")
    original_url = item_package_json.get("url", "")

    text = f"""## {title} ({item_package})


[Original url]({original_url})

"""
    return text


def write_resource(item, item_package_json):
    item_resource_json = [
        i
        for i in item_package_json.get("resources", [])
        if i.get("name") == item["resource_name"]
    ]
    if item_resource_json:
        item_resource_json = item_resource_json[0]
    else:
        item_resource_json = {}

    ia_url_base = "https://archive.org/details/"
    original_url = item_resource_json.get("accessURL", "")
    resource_description = item_resource_json.get("description", "")
    url = urljoin(ia_url_base, item["ia_id"])

    text = f"""### {item["resource_name"]}

{resource_description}

[Internet Archive url]({url}) - [Original url]({original_url})
"""

    return text


def write_portal_readme(portal_name, base_url):

    p_base = Path(portal_name)
    p_internal_md = p_base / "internal_metadata.json"
    p_md = p_base / "metadata"
    p_readme = p_base / "readme.md"

    internal_md = [json.loads(line) for line in p_internal_md.open()]
    internal_md.sort(key=lambda x: x["package_name"])
    internal_md.sort(key=lambda x: x["resource_name"])

    readme = []
    last_package = None

    readme.append(f"# [{portal_name}]({base_url})\n\n")

    for item in internal_md:
        item_package_name = item["package_name"]
        item_package_json = get_package_json(p_md, package_name=item_package_name)
        if not last_package or item_package_name != last_package:
            readme.append(write_package_intro(item_package_name, item_package_json))
            last_package = item_package_name

        readme.append(write_resource(item, item_package_json))

    readme = "\n".join(readme)
    with p_readme.open("w") as f:
        f.write(readme)
