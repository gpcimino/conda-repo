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

from condarepo.package import Package, RepoData
from condarepo.pidfile import PidFile
from condarepo.report import report

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

    report(download_dir, downloaded, num_remote_pkgs, num_local_pkgs)

    if pid_file is not None:
        pid_file.cleanup()

    log.info("Shutting down gracefully")


if __name__ == "__main__":
    main()



