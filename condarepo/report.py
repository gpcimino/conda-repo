import logging

import humanize
from condarepo.utils import get_tree_size


class Report():
    def __init__(self, download_dir, downloaded, num_remote_pkgs, num_local_pkgs, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        self.num_local_pkgs = num_local_pkgs
        self.num_remote_pkgs = num_remote_pkgs
        # num_file_present = sum([1 for p in downloaded if p.file_was_present()])
        self.num_local_pkgs_after = len([f for f in download_dir.glob('*') if f.suffix != ".json"])
        self.num_file_downloaded = sum([1 for p in downloaded if p.was_downloaded()])
        self.num_transfer_error = sum([1 for p in downloaded if p.transfer_error()])
        self.dir_size = get_tree_size(download_dir)
        self.errors = {}
        for e in [str(p.state()) for p in downloaded if p.transfer_error()]:
            self.errors[e] = self.errors.get(e, 0) + 1
        if self.num_file_downloaded > 0:
            self.num_bytes_downloaded = sum([p.file_size() for p in downloaded if p.was_downloaded()])
            self.total_download_time = sum([p.duration_seconds() for p in downloaded if p.was_downloaded()])
            self.max_download_speed = max([p.bandwidth() for p in downloaded if p.was_downloaded()])
            self.min_download_speed = min([p.bandwidth() for p in downloaded if p.was_downloaded()])
            self.average_bandwidth = self.num_bytes_downloaded / self.total_download_time

    def text_report(self, log_name):
        log = logging.getLogger(log_name)
        line = "----------------------------------------------------------------------------------------------"
        log.info("Process start time                                    %s", self.start_time)
        log.info("Process end time                                      %s", self.end_time)
        log.info("Process duration                                      %s", (self.end_time - self.start_time))
        log.info("Number of remote packages                             %s", self.num_remote_pkgs)
        log.info("Number of local packages present before download      %s", self.num_local_pkgs)
        log.info("Packages to download                                  %s", (self.num_remote_pkgs - self.num_local_pkgs))
        log.info("Number of files downloaded                            %s", self.num_file_downloaded)
        log.info("Number of download errors                             %s", self.num_transfer_error)
        for k in self.errors:
            log.info("Number of %s error                                %s", k, self.errors[k])
        log.info("Number of local packages present after download       %s", self.num_local_pkgs_after)
        log.info("Local repository total size after download            %s bytes (%s)", self.dir_size,
                 humanize.naturalsize(self.dir_size))

        if self.num_file_downloaded > 0:
            log.info("Bytes downloaded                                     %s (%s)", self.num_bytes_downloaded,
                     humanize.naturalsize(self.num_bytes_downloaded))
            log.info("Download time                                        %s seconds", self.total_download_time)
            log.info("Max download speed                                   %s bytes/sec (%s/sec)",
                     self.max_download_speed, humanize.naturalsize(self.max_download_speed))
            log.info("Min download speed                                   %s bytes/sec (%s/sec)",
                     self.min_download_speed, humanize.naturalsize(self.min_download_speed))
            log.info("Average download speed                               %s bytes/sec (%s/sec)",
                     self.average_bandwidth, humanize.naturalsize(self.average_bandwidth))

        if self.num_local_pkgs_after <self. num_remote_pkgs:
            log.error(line)
            log.error("Local repository is incomplete")
            log.error(line)
        elif self.num_local_pkgs_after > self.num_remote_pkgs:
            log.warning(line)
            log.warning("[Too many files is local repository something strange happened")
            log.warning(line)
        else:
            log.info(line)
            log.info("Local repository is complete")
            log.info(line)

    def csv_report(self, log_name):
        log = logging.getLogger(log_name)
        log.info("process_start_time,%s", self.start_time)
        log.info("process_end_time,%s", self.end_time)
        log.info("process_duration,%s", (self.end_time - self.start_time))
        log.info("number_of_remote_packages,%s", self.num_remote_pkgs)
        log.info("number_of_local_packages_present_before_download,%s", self.num_local_pkgs)
        log.info("packages_to_download,%s", (self.num_remote_pkgs - self.num_local_pkgs))
        log.info("number_of_files_downloaded,%s", self.num_file_downloaded)
        log.info("number_of_download_errors,%s", self.num_transfer_error)

        for k in self.errors:
            log.info("number_of_error_%s,%s", k.replace(" ", "_"), self.errors[k])

        log.info("number_of_local_packages_present_after_download,%s", self.num_local_pkgs_after)
        log.info("local_repository_total_size_after_download_bytes,%s", self.dir_size)

        if self.num_file_downloaded > 0:
            log.info("bytes_downloaded,%s", self.num_bytes_downloaded,)
            log.info("download_time_seconds,%s ", self.total_download_time)
            log.info("max_download_speed_bytes_per_sec,%s bytes/sec",self.max_download_speed)
            log.info("min_download_speed_bytes_per_sec,%s",self.min_download_speed)
            log.info("average_download_speed_bytes_per_sec,%s)",self.average_bandwidth)
        if self.num_local_pkgs_after <self. num_remote_pkgs:
            log.info("repository_state,incomplete")
        elif self.num_local_pkgs_after > self.num_remote_pkgs:
            log.info("repository_state,too_many_files")
        else:
            log.info("repository_state,complete")

