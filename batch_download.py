from __future__ import print_function

import io
import json
import tarfile
import warnings
import argparse
import os
from glob import glob
from hashlib import md5
from tqdm import tqdm
import multiprocessing as mpl

# for backward compatibility

from utils import mkdir, chunks, extract_month
from scrapers import bs4_scraper, newspaper_scraper, raw_scraper
from download import load_urls, vet_link, download, archive_chunk, get_state, set_state

parser = argparse.ArgumentParser()
parser.add_argument("url_files_folder", type=str)
parser.add_argument(
    "--save_uncompressed",
    action="store_true",
    default=False,
    help="whether to save the raw txt files to disk",
)
parser.add_argument(
    "--output_dir",
    type=str,
    default="scraped",
    help="which folder in the working directory to use for output",
)
parser.add_argument(
    "--n_procs",
    type=int,
    default=1,
    help="how many processes (cores) to use for parallel scraping",
)
parser.add_argument(
    "--timeout",
    type=int,
    default=-1,
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
    default=100,
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


def download_single_url_file(
    url_file, n_procs, output_dir, max_urls, chunk_size, timeout, compress, compress_fmt
):
    month = extract_month(url_file)

    # in case we are resuming from a previous run
    completed_uids, state_fp, prev_cid = get_state(month, output_dir)

    # URLs we haven't scraped yet (if first run, all URLs in file)
    url_entries = load_urls(url_file, completed_uids, max_urls)

    pool = mpl.Pool(n_procs)

    # process one "chunk" of chunk_size URLs at a time
    for i, chunk in enumerate(chunks(url_entries, chunk_size)):
        cid = prev_cid + i + 1

        print("Downloading chunk {}".format(cid))
        t1 = time.time()

        if timeout > 0:
            # imap as iterator allows .next() w/ timeout.
            # ordered version doesn't seem to work correctly.
            # for some reason, you CANNOT track j or chunk[j] in the loop,
            # so don't add anything else to the loop below!
            # confusingly, chunksize below is unrelated to our chunk_size
            chunk_iter = pool.imap_unordered(download, chunk, chunksize=1)
            cdata = []
            for j in range(len(chunk)):
                try:
                    result = chunk_iter.next(timeout=timeout)
                    cdata.append(result)
                except mpl.TimeoutError:
                    print("   --- Timeout Error ---   ")
        else:
            cdata = list(pool.imap(download, chunk, chunksize=1))

        set_state(state_fp, cdata)
        print("{} / {} downloads timed out".format(len(chunk) - len(cdata), len(chunk)))
        print("Chunk time: {} seconds".format(time.time() - t1))

        # archive and save this chunk to file
        if compress:
            print("Compressing...")
            t2 = time.time()
            count = archive_chunk(month, cid, cdata, output_dir, compress_fmt)
            print("Archive created in {} seconds".format(time.time() - t2))
            print("{} out of {} URLs yielded content\n".format(count, len(chunk)))

    print("Done!")


if __name__ == "__main__":
    args.url_files_folder
    """ 
    extract url_files from args.folder
    for url_file in folder:
        download from single url file

    example usage:
    python download.py url_dumps_deduped/RS_20XX-XX.xz.deduped.txt --n_procs 100 --scraper raw --chunk_size 100000 --compress --timeout 30
    """
    for url_file in tqdm(os.listdir(args.url_files_folder)):
        fullpath = os.path.join(args.url_files_folder, url_file)

        download_single_url_file(
            fullpath,
            args.n_procs,
            args.output_dir,
            args.max_urls,
            args.chunk_size,
            args.timeout,
            args.compress,
            args.compress_fmt,
        )
    pass
