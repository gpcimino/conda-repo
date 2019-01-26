# coding: utf-8
import sys
import os
import argparse
import logging
import logging.config
import json
from pathlib import Path
import multiprocessing
from threading import current_thread

import yaml
from furl import furl

from condarepo.package import Package, RepoData
from condarepo.pidfile import PidFile


def update_size(download_size, f):
    log = logging.getLogger("condarepo")
    if not f.exists():
        log.error("Something weired happened, file was downloaded but doesn't exists on disk at %s", f)
        return
    t = current_thread().name
    if t not in download_size:
        download_size[t] = 0
    download_size[t] += f.size

def main():
    #todo: remove pending .tmp-download files
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--thread-number", default=0, type=int, help="Number of parallel threads to use for download and hash computation, default is number of processors cores + 1")
    parser.add_argument("-u", "--repository-url", default='https://repo.continuum.io/pkgs/main/', help="Repository URL, default https://repo.continuum.io/pkgs/main/")
    parser.add_argument("-l", "--logconfig", default=None, help="YAML logger config file, if provided verbose option is ignored")
    parser.add_argument('-v', "--verbose",  default=False, action='store_true', help="Increase log verbosity")
    parser.add_argument('-k', "--keeppackages",  default=False, action='store_true', help="Do not delete local packages which are no longer included in remote repo")
    parser.add_argument('-p', "--pidfile",  default=None, help="File path for file containing process id")
    parser.add_argument('-o', "--timeout",  default=10, type=float, help="HTTP network connnection timeout")
    parser.add_argument("architecture", help="Architecture, one of the follwings: win-64, linux-64,...")
    parser.add_argument("downloaddir", help="Download directory")

    #prepare input parameters
    args = parser.parse_args()

    if args.logconfig is not None:
        with open(args.logconfig) as yamlfile:
            logging.config.dictConfig(yaml.load(yamlfile))
    else:
        if args.verbose:
            logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                                format='%(asctime)s - %(name)s - %(levelname)s  %(message)s')
        else:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                                format='%(asctime)s - %(name)s - %(levelname)s  %(message)s')

    log = logging.getLogger("condarepo")

    architecture = args.architecture
    keeppackages = args.keeppackages
    timeout_sec = args.timeout

    repo_url = furl(args.repository_url).join(architecture + "/")
    download_dir = Path(args.downloaddir) / architecture
    download_dir.makedirs_p()
    optimal_thread_count = multiprocessing.cpu_count() + 1 if args.thread_number==0 else args.thread_number

    log.info("Preparing mirroring repository %s to local directory %s using %s threads", repo_url, download_dir, optimal_thread_count)

    if args.pidfile  is not None:
        pid_file = PidFile(args.pidfile )
        if not pid_file.can_start():
            sys.exit(101)

    # #download remote package list (repodata.json)
    r = RepoData(
        str(args.repository_url),
        architecture,
        local_dir=str(download_dir)
    )
    r.download()
    with open(r.local_filepath()) as data_file:
        repo_data = json.load(data_file)
    log.info("%s contains %s packages", r.local_filepath(), len(repo_data['packages']))


    #look for ".tmp-download" left over files
    previuos_pending_downloads = download_dir.files("*.tmp-download")
    if len(previuos_pending_downloads)>0:
        log.warning(
            "Presumably previous run of condarepo was abruptly aborted, found %s uncompleted tmp download files", len(previuos_pending_downloads))
        for f in previuos_pending_downloads:
            f.remove_p()
            log.warning("Delete uncompleted tmp download files %s", f)

    pkgs = repo_data['packages']

    for name in pkgs:
        try:
            p = Package(str(args.repository_url), name, local_dir=str(download_dir), **pkgs[name])
            p.download()
        except Exception as ex:
            log.error("Cannot download {}".format(p))

    if pid_file is not None:
        pid_file.cleanup()



    # # for i in range(10):
    # #     repo_data['packages'].popitem()
    #
    # #delete local (stale) packages not present in latest repo_data files
    # local_stale_packages = []
    # #exclude repodata.json
    # all_local_packages = [f for f in download_dir.files() if f != repodata_file]
    #
    # log.info("Found %s local packages in %s", len(all_local_packages), download_dir)
    # Observable.from_(all_local_packages) \
    #     .map(lambda f: Path(f)) \
    #     .filter(lambda f: f.ext != ".tmp-download") \
    #     .filter(lambda f: f.name != repodata_file) \
    #     .filter(lambda f: f.name not in repo_data['packages']) \
    #     .subscribe(lambda p: local_stale_packages.append(p))
    # log.info("%s local packages are no longer included in %s", len(local_stale_packages), repo_url)
    #
    #
    # pool_scheduler = ThreadPoolScheduler(optimal_thread_count)
    #
    # latch = config['concurrency'].Event()
    # download_completed_latch = partial(download_completed, latch=latch)
    #
    # # .take(100) \
    # #download_ctr = 0
    # download_size = {}
    # Observable.from_(repo_data['packages'].items()) \
    #     .flat_map( \
    #         lambda s: Observable.just(s) \
    #             .subscribe_on(pool_scheduler) \
    #             .filter(lambda s: need_download(download_dir / s[0], s[1], download_dir)) \
    #             .map(lambda s: download_failsafe(repo_url.copy().join(s[0]), download_dir, timeout_sec=timeout_sec)) \
    #             .filter(lambda s: s is not None) \
    #             .do_action(lambda f: update_size(download_size, f))
    #     ) \
    #     .subscribe( \
    #         on_next=single_file_success_download, \
    #         on_error=lambda e: log.error("Error during download %s", e), \
    #         on_completed=download_completed_latch  \
    #     )
    #
    # log.info("Start checking MD5 CRC and to download packages")
    # latch.wait()
    #
    # log.info("Download is over")
    # log.info("Totally downloaded %s bytes", sum(download_size.values()))
    # log.debug("Data download for each thread:" + str(download_size))
    # #delete stale packages
    # log.info("Delete %s local packages which are no longer included in remote repo", len(local_stale_packages))
    # space_free = 0
    # for f in local_stale_packages:
    #     f = Path(f)
    #     if f.exists():
    #         space_free += f.size
    #         if keeppackages:
    #             log.info("File %s is no longer included in remote repos, but it will be kept locally", f)
    #         else:
    #             f.remove_p()
    #             log.info("File %s deleted", f)
    #     else:
    #         log.warning("File %s no longer exists locally", f)
    # if keeppackages:
    #     log.info("%s bytes of disk space can be freed up if -k switch is used", space_free)
    # else:
    #     log.info("%s bytes of disk space was freed up", space_free)


    log.info("Shutting down gracefully")


if __name__ == "__main__":
    main()



