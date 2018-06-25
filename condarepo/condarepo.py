# coding: utf-8
import sys
import os
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
    log = logging.getLogger("condarepo")
    exists = filepath.exists()
    if exists:
        md5 = filepath.read_hexhash('md5')
        if md5 != fileinfo['md5']:
            log.warn("File %s exists but MD5 hash is wrong, download again", filepath)
            return True
        else:
            log.debug("File %s exists and MD5 hash is OK, no download necessary", filepath)
            return False
    else:
        log.info("File %s not exists locally, start download", filepath)
        return True

def download(url, download_dir):
    log = logging.getLogger("condarepo")
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
    log = logging.getLogger("condarepo")
    log.info("File %s saved", filepath)
    #todo: append to txt file success_download.txt

def download_completed(latch):
    log = logging.getLogger("condarepo")
    latch.set()
    log.info("Download completed")


def main():
    #todo: remove pending .tmp-download files
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--thread-number", default=0, help="Number of parallel threads to use for download and hash computation, default is number of processors cores + 1")
    parser.add_argument("-u", "--repository-url", default='https://repo.continuum.io/pkgs/main/', help="Repository URL, default https://repo.continuum.io/pkgs/main/")
    parser.add_argument("-l", "--logconfig", default=None, help="YAML logger config file, if provided verbose option is ignored")
    parser.add_argument('-v', "--verbose",  default=False, action='store_true', help="Increase log verbosity")
    parser.add_argument('-k', "--keeppackages",  default=False, action='store_true', help="Do not delete local packages which are no longer included in remote repo")
    parser.add_argument('-p', "--pidfile",  default=None, help="File path for file containing process id")
    parser.add_argument("architecture", help="Architecture, one of the follwings: win-64, linux-64,...")
    parser.add_argument("downloaddir", help="Download directory")

    #prepare input parameters
    args = parser.parse_args()

    if args.logconfig is not None:
        with open(args.logconfig) as yamlfile:
            logging.config.dictConfig(yaml.load(yamlfile))
    else:
        # levels  = {1: logging.FATAL, 2: logging.ERROR, 3: logging.WARN, 4: logging.INFO, 5: logging.DEBUG}
        if args.verbose:
            logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                                format='%(asctime)s - %(name)s - %(levelname)s  %(message)s')
        else:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                                format='%(asctime)s - %(name)s - %(levelname)s  %(message)s')
    try:
        log = logging.getLogger("condarepo")

        architecture = args.architecture
        keeppackages = args.keeppackages
        pid_file = Path(args.pidfile) if args.pidfile is not None else None

        repo_url = furl(args.repository_url).join(architecture + "/")
        download_dir = Path(args.downloaddir) / architecture
        download_dir.makedirs_p()
        repodata_file = "repodata.json"
        remote_repodata_file = repo_url.copy().join(repodata_file)
        optimal_thread_count = multiprocessing.cpu_count() + 1 if args.thread_number == 0 else args.thread_number

        log.info("Preparing mirroring repository %s to local directory %s using %s threads", repo_url, download_dir, optimal_thread_count)

        if pid_file is not None:
            if pid_file.exists():
                log.fatal("Found previous pid file %s, something was wrong during last run", pid_file)
                sys.exit(101)
            else:
                pid_file.write_text(str(os.getpid()))
                log.info("Pid file %s created", pid_file)

        #download remote package list (repodata.json)
        local_repo_data_file = download(remote_repodata_file, download_dir)
        with open(local_repo_data_file) as data_file:
            repo_data = json.load(data_file)
        log.info("%s contains %s packages", repodata_file, len(repo_data['packages']))


        #look for ".tmp-download" left over files
        previuos_pending_downloads = download_dir.files("*.tmp-download")
        if len(previuos_pending_downloads)>0:
            log.warning(
                "Presumably previous run of condarepo was abruptly aborted, found %s uncompleted tmp download files", len(previuos_pending_downloads))
            for f in previuos_pending_downloads:
                f.remove_p()
                log.warning("Delete uncompleted tmp download files %s", f)

        # for i in range(10):
        #     repo_data['packages'].popitem()

        #delete local (stale) packages not present in latest repo_data files
        local_stale_packages = []
        #exclude repodata.json
        all_local_packages = [f for f in download_dir.files() if f != repodata_file]

        log.info("Found %s local packages in %s", len(all_local_packages), download_dir)
        Observable.from_(all_local_packages) \
            .map(lambda f: Path(f)) \
            .filter(lambda f: f.ext != ".tmp-download") \
            .filter(lambda f: f.name != repodata_file) \
            .filter(lambda f: f.name not in repo_data['packages']) \
            .subscribe(lambda p: local_stale_packages.append(p))
        log.info("%s local packages are no longer included in %s", len(local_stale_packages), repo_url)


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

        log.info("Start checking MD5 CRC and to download packages")
        latch.wait()

        #delete stale packages
        log.info("Delete %s local packages which are no longer included in remote repo", len(local_stale_packages))
        space_free = 0
        for f in local_stale_packages:
            f = Path(f)
            if f.exists():
                space_free += f.size
                if keeppackages:
                    log.info("File %s is no longer included in remote repos, but it will be kept locally", f)
                else:
                    f.remove_p()
                    log.info("File %s deleted", f)
            else:
                log.warning("File %s no longer exists locally", f)
        if keeppackages:
            log.info("%s bytes of disk space can be freed up if -k switch is used", space_free)
        else:
            log.info("%s bytes of disk space was freed up", space_free)

        pid_file.remove_p()
        log.info("Pid file %s removed", pid_file)
        log.info("Shutting down gracefully")
    except Exception as ex:
        log.exception("General failure")
        sys.exit(100)

if __name__ == "__main__":
    main()



