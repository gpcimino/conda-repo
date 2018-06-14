# coding: utf-8
import sys
from threading import current_thread
import multiprocessing, time, random
import requests
import json
import shutil
import argparse
import logging
import logging.config
from functools import partial

import yaml
from path import Path
from rx import Observable, config
#from rx.core.blockingobservable import BlockingObservable
from rx.concurrency import ThreadPoolScheduler
from furl import furl


def need_download(filepath, fileinfo, download_dir):
    log = logging.getLogger("conda-repo")
    exists = filepath.exists()
    if exists:
        md5 = filepath.read_hexhash('md5')
        if md5 != fileinfo['md5']:
            log.warn("File %s exists but CRC is wrong, download again", filepath)
            return True
        else:
            log.debug("File %s exists and crc is OK, no download necessary", filepath)
            return False
    else:
        log.info("File %s not exists locally, start download", filepath)
        return True

def download(url, download_dir):
    log = logging.getLogger("conda-repo")
    try:
        filename = url.path.segments[-1]
        filepath = download_dir / filename
        filepath_tmp = download_dir / filename + ".tmp-download"
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(filepath_tmp, 'wb') as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
            shutil.move(filepath_tmp, filepath)
            log.debug("File %s downloaded", filepath)
            return filepath
        else:
            log.error("HTTP error %s in download URL %s", r.status_code, url)
    except Exception as ex:
        msg = "Failure in HTTP download for {}".format(url)
        log.exception(msg)
        raise Exception(msg) from ex

def download_failsafe(url, download_dir):
    try:
        return download(url, download_dir)
    except Exception as ex:
        return None

def single_file_success_download(filepath):
    log = logging.getLogger("conda-repo")
    log.info("File %s saved", filepath)
    #todo: append to txt file success_download.txt

def download_completed(latch):
    log = logging.getLogger("conda-repo")
    latch.set()
    log.info("Download completed")


def main():
    #todo: remove pending .tmp-download files
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--thread-number", default=0, help="Number of parallel threads to use for download and hash computation, default is number of processors cores + 1")
    parser.add_argument("-u", "--repository-url", default='https://repo.continuum.io/pkgs/main/', help="Repository URL, default https://repo.continuum.io/pkgs/main/")
    parser.add_argument("-l", "--logconfig", default=None, help="Logger config file")
    parser.add_argument('-v', "--verbose",  default=False, action='store_true', help="Increase log verbosity")
    parser.add_argument("architecture", help="Architecture, one of the follwings: win-64, linux-64,...")
    parser.add_argument("downloaddir", help="Download directory")

    #prepare input parameters
    args = parser.parse_args()
    architecture = args.architecture
    repo_url = furl(args.repository_url).join(architecture + "/")
    download_dir = Path(args.downloaddir) / architecture
    download_dir.makedirs_p()
    repodata_file = "repodata.json"
    remote_repodata_file = repo_url.copy().join(repodata_file)
    optimal_thread_count = multiprocessing.cpu_count() + 1 if args.thread_number == 0 else args.thread_number

    if args.logconfig is not None:
        with open(args.logconfig) as yamlfile:
            logging.config.dictConfig(yaml.load(yamlfile))        
    else:
        #levels  = {1: logging.FATAL, 2: logging.ERROR, 3: logging.WARN, 4: logging.INFO, 5: logging.DEBUG}
        if args.verbose:
            logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        else:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    log = logging.getLogger("conda-repo")
    log.info("Start Mirroring repository %s to local directory %s using %s threads", repo_url, download_dir, optimal_thread_count)

    #download remote package list (repodata.json)
    local_repo_data_file = download(remote_repodata_file, download_dir)
    with open(local_repo_data_file) as data_file:
        repo_data = json.load(data_file)
    log.info("%s contains %s packages", repodata_file, len(repo_data['packages']))


    #delete local (stale) packages not present in latest repo_data files
    local_stale_packages = []
    Observable.from_(download_dir.files()) \
        .map(lambda f: Path(f)) \
        .filter(lambda f: f.name != repodata_file) \
        .filter(lambda f: f.name not in repo_data['packages']) \
        .subscribe(lambda p: local_stale_packages.append(p))
    log.info("%s local packages are not longer included in %s", len(local_stale_packages), repo_url)


    pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

    latch = config['concurrency'].Event()
    download_completed_latch = partial(download_completed, latch=latch)

    # .take(100) \
    #download_ctr = 0
    Observable.from_(repo_data['packages'].items()) \
        .flat_map( \
            lambda s: Observable.just(s) \
                .subscribe_on(pool_scheduler) \
                .filter(lambda s: need_download(download_dir / s[0], s[1], download_dir)) \
                .map(lambda s: download_failsafe(repo_url.copy().join(s[0]), download_dir)) \
                .filter(lambda s: s is not None) \
        ) \
        .subscribe( \
            on_next=single_file_success_download, \
            on_error=lambda e: log.error("Error during download %s", e), \
            on_completed=download_completed_latch  \
        )

    log.info("Wait for threads terminations")
    latch.wait()
    log.info("Download completed ")

    #delete stale packages
    log.info("Delete %s local packages are not longer included in remote repo", len(local_stale_packages))
    space_free = 0
    for f in local_stale_packages:
        f = Path(f)
        if f.exists():
            f.remove_p()
            log.debug("File %s deleted", f)
            space_free += f.size()
        else:
            log.warning("File %s no longer exists locally", f)

    log.info("Freed %s bytes on disk", space_free)
    #input("press a key\n")
    #log.info("Downloaded %s files", download_ctr)


if __name__ == "__main__":
    main()



