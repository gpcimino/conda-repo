import logging

import humanize
from condarepo.utils import get_tree_size


log = logging.getLogger("condarepo.report")


def report(download_dir, downloaded, num_remote_pkgs, num_local_pkgs):
    # num_file_present = sum([1 for p in downloaded if p.file_was_present()])
    num_local_pkgs_after = len([f for f in download_dir.glob('*') if f.suffix != ".json"])
    num_file_downloaded = sum([1 for p in downloaded if p.was_downloaded()])
    num_transfer_error = sum([1 for p in downloaded if p.transfer_error()])
    dir_size = get_tree_size(download_dir)

    errors = {}
    for e in [str(p.state()) for p in downloaded if p.transfer_error()]:
        errors[e] = errors.get(e, 0) + 1

    log.info("Number of remote packages                            %s", num_remote_pkgs)
    log.info("Number of local packages present before download     %s", num_local_pkgs)
    log.info("Packages to download                                 %s", (num_remote_pkgs - num_local_pkgs))
    log.info("Number of files downloaded                           %s", num_file_downloaded)
    log.info("Number of download errors                            %s", num_transfer_error)
    for k in errors:
        log.info("Number of %s error                               %s", k, errors[k])
    log.info("Number of local packages present after download      %s", num_local_pkgs_after)
    log.info("Local repository total size after download           %s bytes (%s)", dir_size,
             humanize.naturalsize(dir_size))

    if num_file_downloaded > 0:
        num_bytes_downloaded = sum([p.file_size() for p in downloaded if p.was_downloaded()])
        total_download_time = sum([p.duration_seconds() for p in downloaded if p.was_downloaded()])
        max_download_speed = max([p.bandwidth() for p in downloaded if p.was_downloaded()])
        min_download_speed = min([p.bandwidth() for p in downloaded if p.was_downloaded()])
        average_bandwidth = num_bytes_downloaded / total_download_time
        log.info("Bytes downloaded                                     %s (%s)", num_bytes_downloaded,
                 humanize.naturalsize(num_bytes_downloaded))
        log.info("Download time                                        %s seconds", total_download_time)
        log.info("Max download speed                                   %s bytes/sec (%s/sec)",
                 max_download_speed, humanize.naturalsize(max_download_speed))
        log.info("Min download speed                                   %s bytes/sec (%s/sec)",
                 min_download_speed, humanize.naturalsize(min_download_speed))
        log.info("Average download speed                               %s bytes/sec (%s/sec)",
                 average_bandwidth, humanize.naturalsize(average_bandwidth))

    if num_local_pkgs_after < num_remote_pkgs:
        log.error(
            "----------------------------------------------------------------------------------------------")
        log.error("Local repository is incomplete")
        log.error(
            "----------------------------------------------------------------------------------------------")
    elif num_local_pkgs_after > num_remote_pkgs:
        log.warning(
            "----------------------------------------------------------------------------------------------")
        log.warning("[Too many files is local repository something strange happened")
        log.warning(
            "----------------------------------------------------------------------------------------------")
    else:
        log.info(
            "----------------------------------------------------------------------------------------------")
        log.info("Local repository is complete")
        log.info(
            "----------------------------------------------------------------------------------------------")


