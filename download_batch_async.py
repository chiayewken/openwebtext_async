import asyncio
import math
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import List

import firebase_admin
from aiohttp import ClientSession
from firebase_admin import credentials, db
from tqdm import tqdm

# def newspaper_get_html(url):
#     try:
#         a = newspaper.Article(url)
#         a.download()
#         html_string = a.html

#         if is_html(html_string):
#             with open(generate_name(".html"), "w") as file:
#                 file.write(html_string)
#         return html_string
#     except Exception:
#         return None


# def newspaper_fetch_htmls(urls):
#     # slow sequential version
#     return [newspaper_get_html(url) for url in tqdm(urls)]


# def pool_newspaper_fetch_htmls(urls, poolsize=None):
#     # poolsize 50 seems to be best
#     with Pool(poolsize) as pool:
#         return list(tqdm(pool.imap_unordered(newspaper_get_html, urls)))


async def fetch(url: str, session) -> str:
    try:
        # timeout 7s -> 1min for 10k urls (4k success)
        # timeout 30s -> 3min for 10k urls (7k success)
        async with session.get(url, allow_redirects=True, timeout=30) as r:
            return await r.text()
    except Exception:
        return ""


async def bound_fetch(sem: asyncio.Semaphore, url: str, session: ClientSession) -> str:
    # Getter function with semaphore.
    async with sem:
        return await fetch(url, session)


async def fetch_htmls_loop(urls: List[str], hide_progress: bool) -> List[str]:
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
        for t in tqdm(
            asyncio.as_completed(tasks), total=len(tasks), disable=hide_progress
        ):
            responses.append(await t)
        return responses


def fetch_htmls(urls: List[str], hide_progress: bool) -> List[str]:
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(fetch_htmls_loop(urls, hide_progress))
    return loop.run_until_complete(future)


def is_html(string: str) -> bool:
    # return bool(BeautifulSoup(string, "html.parser").find())  # slow
    return "html" in string  # and len(string) > 1000


def generate_name(suffix: str = "") -> str:
    tempfile.tempdir = tempfile.template = ""
    return tempfile.mktemp(suffix)


def save_htmls(htmls: List[str], save_dir: Path) -> None:
    for h in htmls:
        if is_html(h):
            path = save_dir.joinpath(generate_name(".html"))
            with open(str(path), "w") as file:
                file.write(h)


def archive(path_in: Path, path_out: Path, compress: bool = True) -> Path:
    mode = {False: "w", True: "w:gz"}[compress]
    suffix = {False: ".tar", True: ".tgz"}[compress]
    path_out = path_out.with_suffix(suffix)
    with tarfile.open(path_out, mode) as tar:
        tar.add(path_in)
    return path_out


def count_total_lines(file: str) -> int:
    count = 0
    with open(file) as f:
        for line in f:
            count += 1
    return count


def count_batches(total: int, batch_size: int) -> int:
    return math.ceil(total / batch_size)


def firebase_init(gdrive_dir: Path) -> None:
    path_json, database_url = open(
        str(gdrive_dir.joinpath("firebase_details.txt"))
    ).readlines()
    cred = credentials.Certificate(str(gdrive_dir.joinpath(path_json.strip())))
    firebase_admin.initialize_app(cred, {"databaseURL": database_url.strip()})
    print("firebase initialized")


def firebase_check_exists(id: str) -> bool:
    database = db.reference().get()
    # firebase acts like empty array when empty (nonetype iteration error)
    return database != None and str(id) in database


def firebase_set(id: str, value: bool) -> None:
    db.reference(str(id)).set(value)


def get_batch_urls(urls_file: str, idx: int, batch_size: int) -> List[str]:
    # must avoid out of range error
    batch_range = range(idx * batch_size, (idx + 1) * batch_size)
    with open(urls_file) as f:
        return [x.strip() for (i, x) in enumerate(f) if i in batch_range]


def firebase_sync_gdrive(gdrive_dir: Path, save_dir: Path) -> None:
    # manually align firebase entries to gdrive archives
    batch_names = [p for p in gdrive_dir.joinpath(save_dir).iterdir()]
    dict_new = {p.stem: True for p in batch_names}
    dict_old = db.reference().get()
    # sanity check: each archive should already be listed in firebase
    assert all(key in dict_old for key in dict_new)
    db.reference().set(dict_new)


def main(
    gdrive_dir: Path,
    batch_size: int = 100000,
    hide_tracebacks: bool = True,
    hide_progress: bool = True,
) -> None:
    """
    Init firebase and setup directories
    For every possible batch idx:
        Check if exists in firebase
        Get batch_size num lines of url strings from 2gb urls.txt in gdrive
        Async fetch html contents
        Save and archive
        Send to google drive
        Delete temp files (archive)
    """
    if hide_tracebacks:
        sys.tracebacklimit = 0  # suppress url error reports

    firebase_init(gdrive_dir)
    urls_file = gdrive_dir.joinpath("urls.txt")
    save_dir = Path("downloads")
    save_dir.mkdir(exist_ok=True)
    gdrive_dir.joinpath(save_dir).mkdir(exist_ok=True)
    num_total_urls = count_total_lines(str(urls_file))
    num_total_batches = count_batches(num_total_urls, batch_size)
    print("num_total_urls:", num_total_urls)
    print("num_total_batches", num_total_batches)

    for batch_idx in range(num_total_batches):
        # this format will allow us to recover exact line indices if necessary
        batch_name = "{}_{}".format(batch_size, batch_idx)
        if firebase_check_exists(batch_name):
            continue  # skip

        print("handling batch:", batch_name)
        firebase_set(batch_name, True)
        urls = get_batch_urls(str(urls_file), batch_idx, batch_size)
        # should hide tqdm for long iterations because the output can crash notebook
        htmls = fetch_htmls(urls, hide_progress)
        save_htmls(htmls, save_dir)
        archive_fname = archive(save_dir, Path(batch_name), compress=True)
        shutil.move(
            str(archive_fname), str(gdrive_dir.joinpath(save_dir, archive_fname))
        )
        shutil.rmtree(save_dir)
        save_dir.mkdir()
