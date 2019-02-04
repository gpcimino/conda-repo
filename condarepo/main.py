# coding: utf-8
import sys
import argparse
import logging
import logging.config
import json
from pathlib import Path
from multiprocessing import Pool, cpu_count
import functools

import yaml
from furl import furl
import humanize

from condarepo.package import Package, RepoData
from condarepo.pidfile import PidFile
from condarepo.utils import get_tree_size

def download(p, timeout_sec):
    log = logging.getLogger("condarepo")
    try:
        p.download(timeout_sec=timeout_sec)
        return p
    except Exception as ex:
        log.exception("File download %s aborted after retries. This file will be missing from local repo", p.url())

def main():
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

    # prepare input parameters
    args = parser.parse_args()

    if args.logconfig is not None:
        with open(args.logconfig) as yamlfile:
            logging.config.dictConfig(yaml.load(yamlfile))
    else:
        if args.verbose:
            logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                                format='%(asctime)s - %(name)s - [%(process)d] %(levelname)s  %(message)s')
        else:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                                format='%(asctime)s - %(name)s - [%(process)d] %(levelname)s  %(message)s')

    log = logging.getLogger("condarepo")

    architecture = args.architecture
    keeppackages = args.keeppackages
    timeout_sec = args.timeout
    baseurl = args.repository_url
    repo_url = furl(baseurl).join(architecture + "/")
    download_dir = Path(args.downloaddir) / architecture
    download_dir.mkdir(parents=True, exist_ok=True)

    optimal_thread_count = cpu_count() + 1 if args.thread_number==0 else args.thread_number

    log.info("Preparing mirroring repository %s to local directory %s using %s threads", repo_url, download_dir, optimal_thread_count)

    if args.pidfile is not None:
        pid_file = PidFile(args.pidfile )
        if not pid_file.can_start():
            sys.exit(101)
    else:
        pid_file = None

    # download remote package list (repodata.json)
    r = RepoData(
        str(repo_url),
        local_dir=download_dir
    )
    r.download(timeout_sec=timeout_sec)
    with open(r.local_filepath()) as data_file:
        repo_data = json.load(data_file)
    log.info("%s contains %s packages refs", r.local_filepath(), len(repo_data['packages']))


    # look for ".tmp-download" left over files
    for f in download_dir.glob("*.tmp-download"):
        f.unlink()
        log.warning("Presumably previous run of condarepo was abruptly aborted, found and deleted uncompleted tmp download files %s", f)


    # count pkgs on disk
    num_local_pkgs = len([f for f in download_dir.glob('*') if f.suffix != ".json"])
    num_remote_pkgs=len(repo_data['packages'])
    log.info("Found %s local packages in %s", num_local_pkgs, download_dir)
    log.info("Found %s remote packages in %s", num_remote_pkgs, repo_url)
    log.info("Packages to download %s", (num_remote_pkgs-num_local_pkgs))

    # start download
    p = Pool(optimal_thread_count)
    pkgs = repo_data['packages']
    pkgs = [Package(str(repo_url), name, local_dir=download_dir, **pkgs[name]) for name in pkgs]
    download_func = functools.partial(download, timeout_sec=timeout_sec)
    downloaded = p.map(download_func, pkgs)

    # num_file_present = sum([1 for p in downloaded if p.file_was_present()])
    num_local_pkgs_after = len([f for f in download_dir.glob('*') if f.suffix != ".json"])
    num_file_downloaded = sum([1 for p in downloaded if p.was_downloaded()])
    num_transfer_error = sum([1 for p in downloaded if p.transfer_error()])
    dir_size = get_tree_size(download_dir)

    errors = {}
    for e in [str(p.state()) for p in downloaded if p.transfer_error()]:
        errors[e] = errors.get(e, 0) + 1

    #log.info("[REPORT] Number of files present (no download necessary) %s", num_file_present)

    log.info("[REPORT] Number of remote packages                            %s", num_remote_pkgs)
    log.info("[REPORT] Number of local packages present before download     %s", num_local_pkgs)
    log.info("[REPORT] Packages to download                                 %s", (num_remote_pkgs-num_local_pkgs))
    log.info("[REPORT] Number of files downloaded                           %s", num_file_downloaded)
    log.info("[REPORT] Number of download errors                            %s", num_transfer_error)
    for k in errors:
        log.info("[REPORT] Number of %s error                               %s", k, errors[k])
    log.info("[REPORT] Number of local packages present after download      %s", num_local_pkgs_after)
    log.info("[REPORT] Local repository total size after download           %s bytes (%s)", dir_size, humanize.naturalsize(dir_size))

    if num_file_downloaded > 0:
        num_bytes_downloaded = sum([p.file_size() for p in downloaded if p.was_downloaded()])
        total_download_time = sum([p.duration_seconds() for p in downloaded if p.was_downloaded()])
        max_download_speed = max([p.bandwidth() for p in downloaded if p.was_downloaded()])
        min_download_speed = min([p.bandwidth() for p in downloaded if p.was_downloaded()])
        average_bandwidth = num_bytes_downloaded / total_download_time
        log.info("[REPORT] Bytes downloaded                                     %s (%s)", num_bytes_downloaded, humanize.naturalsize(num_bytes_downloaded))
        log.info("[REPORT] Download time                                        %s seconds", total_download_time)
        log.info("[REPORT] Max download speed                                   %s bytes/sec (%s/sec)", max_download_speed, humanize.naturalsize(max_download_speed))
        log.info("[REPORT] Min download speed                                   %s bytes/sec (%s/sec)", min_download_speed, humanize.naturalsize(min_download_speed))
        log.info("[REPORT] Average download speed                               %s bytes/sec (%s/sec)", average_bandwidth, humanize.naturalsize(average_bandwidth))

    if num_local_pkgs_after < num_remote_pkgs:
        log.error("[REPORT] ----------------------------------------------------------------------------------------------")
        log.error("[REPORT] Local repository is incomplete")
        log.error("[REPORT] ----------------------------------------------------------------------------------------------")
    elif num_local_pkgs_after > num_remote_pkgs:
        log.warning("[REPORT] ----------------------------------------------------------------------------------------------")
        log.warning("[REPORT] Too many files is local repository something strange happened")
        log.warning("[REPORT] ----------------------------------------------------------------------------------------------")
    else:
        log.info("[REPORT] ----------------------------------------------------------------------------------------------")
        log.info("[REPORT] Local repository is complete")
        log.info("[REPORT] ----------------------------------------------------------------------------------------------")

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



