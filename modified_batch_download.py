from __future__ import print_function

import argparse
import os
from tqdm import tqdm
from download import main as download_single_url_file


def main(args):
    """ 
    extract url_files from args.folder
    for url_file in folder:
        download from single url file
    """
    print("downloading from urls in: {}".format(args.urls_dir))
    print("saving downloaded chunks to: {}".format(args.output_dir))
    for url_file in tqdm(os.listdir(args.urls_dir)):
        fullpath = os.path.join(args.urls_dir, url_file)
        download_single_url_file(fullpath, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--urls_dir",
        type=str,
        default="urls",
        help="folder that contains url txt files",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="scraped",
        help="which folder in the working directory to use for output",
    )
    parser.add_argument(
        "--save_uncompressed",
        action="store_true",
        default=False,
        help="whether to save the raw txt files to disk",
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
        help="maximum scrape time (s) for a single URL; -1 means no limit",
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
    main(args)
