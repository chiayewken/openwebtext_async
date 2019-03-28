from __future__ import print_function

import io
import time
import json
import tarfile
import warnings
import argparse
import shutil
import os.path as op
from glob import glob
from hashlib import md5
from pathlib import Path
import multiprocessing as mpl

import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

# for backward compatibility
from six.moves.urllib.request import urlopen

from utils import mkdir, chunks, extract_month
from scrapers import bs4_scraper, newspaper_scraper, raw_scraper

parser = argparse.ArgumentParser()
parser.add_argument("gdrive_dir", type=str)
parser.add_argument("--url_file", type=str)
parser.add_argument(
    "--save_uncompressed",
    action="store_true",
    default=True,
    help="whether to save the raw txt files to disk",
)
parser.add_argument(
    "--output_dir",
    type=str,
    default="scraped",
    help="which folder in the working directory to use for output",
)
parser.add_argument(
    "--state_dir",
    type=str,
    default="state",
    help="which folder in the working directory to use for storing progress",
)
parser.add_argument(
    "--n_procs",
    type=int,
    default=100,
    help="how many processes (cores) to use for parallel scraping",
)
parser.add_argument(
    "--timeout",
    type=int,
    default=30,
    help="maximum scrape time for a single URL; -1 means no limit",
)
parser.add_argument(
    "--max_urls",
    type=int,
    default=-1,
    help="maximum # of URLs to scrape; mostly for debugging",
)
parser.add_argument(
    "--chunk_size",
    type=int,
    default=100000,
    help="how many URLs to scrape before saving to archive",
)
parser.add_argument(
    "--scraper",
    type=str,
    default="newspaper",
    choices=["raw", "bs4", "newspaper"],
    help="which text/content scraper to use; raw is html",
)
parser.add_argument(
    "--compress",
    action="store_true",
    default=False,
    help="whether to output scraped content as compressed archives",
)
parser.add_argument(
    "--compress_fmt",
    type=str,
    default="xz",
    choices=["xz", "bz2", "gz"],
    help="which archive format to use",
)
parser.add_argument(
    "--scraper_memoize",
    action="store_true",
    default=False,
    help="whether to use cache for newspaper",
)
parser.add_argument(
    "--show_warnings",
    action="store_true",
    default=False,
    help="whether to show warnings in general during scraping",
)
args = parser.parse_args()

if not args.show_warnings:
    # avoid lots of datetime warnings
    warnings.filterwarnings("ignore")


def load_urls(url_file, completed_fids, max_urls=-1):
    with open(url_file) as fh:
        url_entries = [
            (fid, url) for (fid, url) in enumerate(fh) if fid not in completed_fids
        ]
        if max_urls != -1:
            url_entries = url_entries[:max_urls]
    return url_entries


def vet_link(link):
    # check if server responds with non-200 status code or link points to a
    # non-html file
    link_type, link_status = "", -1
    try:
        info = urlopen(link)
        link_type = info.headers["Content-Type"]
        link_status = info.status
    except:
        pass

    # we want "text/html" only!
    is_good_link = False
    if "text/html" in link_type and link_status == 200:
        is_good_link = True

    return is_good_link, link_type


def download(
    url_entry,
    scraper=args.scraper,
    save_uncompressed=args.save_uncompressed,
    memoize=args.scraper_memoize,
):
    uid, url = url_entry
    url = url.strip()
    fid = "{:07d}-{}".format(uid, md5(url.encode()).hexdigest())

    # is_good_link, link_type = vet_link(url)
    # if not is_good_link:
    #     return

    if scraper == "bs4":
        scrape = bs4_scraper
    elif scraper == "newspaper":
        scrape = newspaper_scraper
    elif scraper == "raw":
        scrape = raw_scraper

    text, meta = scrape(url, memoize)
    if text is None or text.strip() == "":
        return ("", "", fid, uid)

    if save_uncompressed:
        month = extract_month(args.url_file)
        data_dir = mkdir(op.join(args.output_dir, "data", month))
        meta_dir = mkdir(op.join(args.output_dir, "meta", month))
        text_fp = op.join(data_dir, "{}.txt".format(fid))
        meta_fp = op.join(meta_dir, "{}.json".format(fid))

        with open(text_fp, "w") as out:
            out.write(text)
        with open(meta_fp, "w") as out:
            json.dump(meta, out)

    return (text, meta, fid, uid)


def archive_chunk(month, cid, cdata, out_dir, fmt):
    mkdir(out_dir)
    texts, metas, fids, uids = zip(*cdata)

    data_tar = op.join(out_dir, "{}-{}_data.{}".format(month, cid, fmt))
    meta_tar = op.join(out_dir, "{}-{}_meta.{}".format(month, cid, fmt))
    tar_fps, texts, exts = [data_tar, meta_tar], [texts, metas], ["txt", "json"]

    doc_count = 0
    docs_counted = False
    for tar_fp, txts, ext in zip(tar_fps, texts, exts):
        with tarfile.open(tar_fp, "w:" + fmt) as tar:
            for f, fid in zip(txts, fids):
                if f == "":
                    continue
                else:
                    if not docs_counted:
                        doc_count += 1

                if ext == "json":
                    f = json.dumps(f)

                f = f.encode("utf-8")
                t = tarfile.TarInfo("{}.{}".format(fid, ext))
                t.size = len(f)
                tar.addfile(t, io.BytesIO(f))
        docs_counted = True

    return doc_count


#######################################################################
#                           Util functions                            #
#######################################################################


def get_state(month, out_dir):
    mkdir(args.state_dir)
    latest_cid = 0
    completed_uids = set()
    state_fp = op.join(args.state_dir, "{}.txt".format(month))
    if op.isfile(state_fp):
        archives = glob(op.join(out_dir, "{}-*".format(month)))
        latest_cid = max([int(a.split("-")[-1].split("_")[0]) for a in archives])
        with open(state_fp, "r") as fh:
            completed_uids = set(int(i.strip()) for i in list(fh))
    return completed_uids, state_fp, latest_cid


def set_state(state_fp, cdata):
    _, _, _, uids = zip(*cdata)
    with open(state_fp, "a+") as handle:
        for uid in uids:
            handle.write("{}\n".format(uid))


def namify(x):
    # drop suffixes eg dir/file.txt -> file
    return str(x).split("/")[-1].split(".")[0]


def firebase_init(gdrive_dir):
    path_json, database_url = open(
        gdrive_dir.joinpath("firebase_details.txt")
    ).readlines()
    cred = credentials.Certificate(str(gdrive_dir.joinpath(path_json.strip())))
    firebase_admin.initialize_app(cred, {"databaseURL": database_url.strip()})
    print("firebase initialized")


def firebase_batch_status_update(gdrive_dir):
    urls_dir = gdrive_dir.joinpath("urls")
    downloads_dir = gdrive_dir.joinpath("downloads")
    urls = [p for p in urls_dir.iterdir() if p.is_file()]
    for url in urls:
        name = namify(url)
        path_output = downloads_dir.joinpath(name)
        db.reference().child(name).set(path_output.exists())


def firebase_check_status_url_file(url):
    return db.reference(namify(url)).get()


def firebase_set_status_url_file(url, status):
    assert type(status) == bool, "status value must be boolean"
    db.reference(namify(url)).set(status)
    print("{} handled status set to: {}".format(url, status))


if __name__ == "__main__":
    """
    obtain list of urls from gdrive urls folder
    for each url file path:
        check that destination folder for this url file does not exist
        create destination folder for this url file
        delete any previous local files
        run download for this url file
        compress local files
        send archive to google drive
    """
    gdrive_dir = Path(args.gdrive_dir)
    urls_dir = gdrive_dir.joinpath("urls")
    downloads_dir = gdrive_dir.joinpath("downloads")
    downloads_dir.mkdir(exist_ok=True)
    urls = [p for p in urls_dir.iterdir() if p.is_file()]
    print("urls found: ", len(urls))
    local_folders = [args.output_dir, args.state_dir]
    firebase_init(gdrive_dir)

    for url in urls:
        if firebase_check_status_url_file(url) is True:
            continue  # url file has been handled; skip

        firebase_set_status_url_file(url, True)
        path_output = downloads_dir.joinpath(namify(url))
        path_output.mkdir()

        for folder in local_folders:
            shutil.rmtree(folder, ignore_errors=True)

        #################### start of single url download ####################
        args.url_file = url  # download() depends on global level arg variable
        month = extract_month(args.url_file)
        # in case we are resuming from a previous run
        completed_uids, state_fp, prev_cid = get_state(month, args.output_dir)
        # URLs we haven't scraped yet (if first run, all URLs in file)
        url_entries = load_urls(args.url_file, completed_uids, args.max_urls)
        pool = mpl.Pool(args.n_procs)

        # process one "chunk" of args.chunk_size URLs at a time
        for i, chunk in enumerate(chunks(url_entries, args.chunk_size)):
            cid = prev_cid + i + 1

            print("Downloading chunk {}".format(cid))
            t1 = time.time()

            if args.timeout > 0:
                # imap as iterator allows .next() w/ timeout.
                # ordered version doesn't seem to work correctly.
                # for some reason, you CANNOT track j or chunk[j] in the loop,
                # so don't add anything else to the loop below!
                # confusingly, chunksize below is unrelated to our chunk_size
                chunk_iter = pool.imap_unordered(download, chunk, chunksize=1)
                cdata = []
                for j in range(len(chunk)):
                    try:
                        result = chunk_iter.next(timeout=args.timeout)
                        cdata.append(result)
                    except mpl.TimeoutError:
                        print("   --- Timeout Error ---   ")
            else:
                cdata = list(pool.imap(download, chunk, chunksize=1))

            set_state(state_fp, cdata)
            print(
                "{} / {} downloads timed out".format(
                    len(chunk) - len(cdata), len(chunk)
                )
            )
            print("Chunk time: {} seconds".format(time.time() - t1))

            # archive and save this chunk to file
            if args.compress:
                print("Compressing...")
                t2 = time.time()
                count = archive_chunk(
                    month, cid, cdata, args.output_dir, args.compress_fmt
                )
                print("Archive created in {} seconds".format(time.time() - t2))
                print("{} out of {} URLs yielded content\n".format(count, len(chunk)))

        print("Done!")
        #################### end of single url download ####################

        tar = tarfile.open(path_output.joinpath("scraped.tar"), "w")
        for folder in local_folders:
            tar.add(folder)
        tar.close()
