import asyncio
from aiohttp import ClientSession
from pathlib import Path
from tqdm import tqdm
import tarfile
import shutil
import math
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import tempfile


async def fetch(url, session):
    try:
        # timeout 7s -> 1min for 10k urls (4k success)
        # timeout 30s -> 3min for 10k urls (7k success)
        async with session.get(url, allow_redirects=True, timeout=30) as r:
            return await r.text()
    except Exception:
        return ""


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(url, session)


async def fetch_htmls_loop(urls):
    # reference: https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
    tasks = []
    # Semaphore to avoid python open file limit
    sem = asyncio.Semaphore(1000)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for idx, url in enumerate(urls):
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)

        # newspaper.fulltext is very slow (5min/1000urls) (18mb)
        # best to save raw html
        # responses = asyncio.gather(*tasks)
        # await responses
        responses = []
        for t in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            responses.append(await t)
        return responses


def fetch_htmls(urls):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(fetch_htmls_loop(urls))
    return loop.run_until_complete(future)


def is_html(string):
    # return bool(BeautifulSoup(string, "html.parser").find())  # slow
    return "html" in string  # and len(string) > 1000


def generate_name(suffix=""):
    tempfile.tempdir = tempfile.template = ""
    return tempfile.mktemp(suffix)


def save_htmls(htmls, save_dir):
    for h in htmls:
        if is_html(h):
            path = save_dir.joinpath(generate_name(".html"))
            with open(path, "w") as file:
                file.write(h)


def archive(path_in, path_out):
    with tarfile.open(path_out, "w") as tar:
        tar.add(path_in)


def count_total_lines(file):
    count = 0
    with open(file) as f:
        for line in f:
            count += 1
    return count


def count_batches(total, batch_size):
    return math.ceil(total / batch_size)


def firebase_init(work_dir):
    path_json, database_url = open(
        work_dir.joinpath("firebase_details.txt")
    ).readlines()
    cred = credentials.Certificate(str(work_dir.joinpath(path_json.strip())))
    firebase_admin.initialize_app(cred, {"databaseURL": database_url.strip()})
    print("firebase initialized")


def firebase_check_exists(id):
    return id in db.reference().get()


def firebase_set(id, value):
    db.reference(id).set(value)


def get_batch_urls(urls_file, idx, batch_size):
    # must avoid out of range error
    batch_range = range(idx * batch_size, (idx + 1) * batch_size)
    with open(urls_file) as f:
        return [x.strip() for (i, x) in enumerate(f) if i in batch_range]
