import asyncio
import math
import shutil
import sys
import tarfile
import tempfile
import newspaper
from pathlib import Path

import firebase_admin
from aiohttp import ClientSession
from firebase_admin import credentials, db
from tqdm import tqdm
from multiprocessing import Pool


sys.tracebacklimit = 0  # suppress url error reports


def newspaper_get_html(url):
    try:
        a = newspaper.Article(url)
        a.download()
        html_string = a.html

        if is_html(html_string):
            with open(generate_name(".html"), "w") as file:
                file.write(html_string)
        return html_string
    except Exception:
        return None


def newspaper_fetch_htmls(urls):
    # slow sequential version
    return [newspaper_get_html(url) for url in tqdm(urls)]


def pool_newspaper_fetch_htmls(urls, poolsize=None):
    # poolsize 50 seems to be best
    with Pool(poolsize) as pool:
        return list(tqdm(pool.imap_unordered(newspaper_get_html, urls)))


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
        for t in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
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


def archive(path_in, path_out, compress=True):
    mode = {False: "w", True: "w:gz"}[compress]
    suffix = {False: ".tar", True: ".tgz"}[compress]
    path_out = path_out.with_suffix(suffix)
    with tarfile.open(path_out, mode) as tar:
        tar.add(path_in)
    return path_out


def count_total_lines(file):
    count = 0
    with open(file) as f:
        for line in f:
            count += 1
    return count


def count_batches(total, batch_size):
    return math.ceil(total / batch_size)


def firebase_init(gdrive_dir):
    path_json, database_url = open(
        gdrive_dir.joinpath("firebase_details.txt")
    ).readlines()
    cred = credentials.Certificate(str(gdrive_dir.joinpath(path_json.strip())))
    firebase_admin.initialize_app(cred, {"databaseURL": database_url.strip()})
    print("firebase initialized")


def firebase_check_exists(id):
    return str(id) in db.reference().get()


def firebase_set(id, value):
    db.reference(str(id)).set(value)


def get_batch_urls(urls_file, idx, batch_size):
    # must avoid out of range error
    batch_range = range(idx * batch_size, (idx + 1) * batch_size)
    with open(urls_file) as f:
        return [x.strip() for (i, x) in enumerate(f) if i in batch_range]


def main(gdrive_dir, batch_size=100000):
    """
    init
    get chunks
    fetch htmls
    save and archive
    send to google drive
    delete archive
    """
    firebase_init(gdrive_dir)
    save_dir = Path("downloads")
    save_dir.mkdir(exist_ok=True)
    gdrive_dir.joinpath(save_dir).mkdir(exist_ok=True)
    num_total_urls = count_total_lines(gdrive_dir.joinpath("urls.txt"))
    num_total_batches = count_batches(num_total_urls, batch_size)
    print("num_total_urls:", num_total_urls)
    print("num_total_batches", num_total_batches)

    for batch_idx in range(num_total_batches):
        # this format will allow us to recover exact line indices if necessary
        batch_name = "{}_{}".format(batch_size, batch_idx)
        print("handling batch:", batch_name)
        if firebase_check_exists(batch_name):
            continue  # skip

        firebase_set(batch_name, True)
        urls = get_batch_urls(batch_idx, batch_size)
        htmls = fetch_htmls(urls)
        save_htmls(htmls, save_dir)
        archive_fname = archive(save_dir, Path(batch_name), compress=True)
        shutil.move(archive_fname, gdrive_dir.joinpath(save_dir, archive_fname))
        shutil.rmtree(save_dir)
