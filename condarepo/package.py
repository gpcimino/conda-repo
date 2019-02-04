import tempfile
from pathlib import Path
from furl import furl
import shutil
import logging
from datetime import datetime
import hashlib
import time

import humanize
import requests
from requests.exceptions import RequestException

log = logging.getLogger("condarepo")

class Status():
    pass


class DownloadOK(Status):
    def __str__(self):
        return "OK"

    def ok(self):
        return True


class FileAlreadyPresent(Status):
    def __str__(self):
        return "File present"

    def ok(self):
        return True


class HTTPError(Status):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return "HTTP Error " + str(self.code)

    def ok(self):
        return False


class BadCRC(Status):
    def __str__(self):
        return "Bad CRC"

    def ok(self):
        return False


class ConnectionError(Status):
    def __init__(self, ex):
        self.ex = ex

    def __str__(self):
        return self.ex.msg

    def ok(self):
        return False


class GenericError(Status):
    def __init__(self, ex):
        self.ex = ex

    def __str__(self):
        return self.ex.msg

    def ok(self):
        return False

class NotStarted(Status):
    def ok(self):
        return False


class Package():
    """
      "build": "py27_0",
      "build_number": 0,
      "date": "2013-03-01",
      "depends": [
        "python 2.7*"
      ],
      "license": "proprietary - Continuum Analytics, Inc.",
      "license_family": "Proprietary",
      "md5": "4ced1f80ffe9ed609d55da8dd52b63bd",
      "name": "_license",
      "size": 50872,
      "version": "1.1"
"""
    def __init__(self, base_url, filename, local_dir=tempfile.mkdtemp(prefix="condarepo", dir="/tmp/"), **kwargs):
        self._base_url = furl(base_url)
        self.filename = filename
        self._info = kwargs
        self._local_dir = Path(local_dir)
        self._state = NotStarted()
        self._duration = None
        self._max_retry = 1
        self._maximum_backoff = 60

    def url(self):
        return str(self._base_url.copy().join(self.filename))

    def local_filepath(self):
        return self._local_dir / self.filename

    def local_tmp_filepath(self):
        return self.local_filepath().with_suffix(".tmp-download")

    def file_exists_locally(self):
        return self.local_filepath().exists()

    def file_size(self):
        return self.local_filepath().stat().st_size

    def human_file_size(self):
        return humanize.naturalsize(self.file_size())

    def download(self, timeout_sec=10):

        if self.file_exists_locally():
            self._state = FileAlreadyPresent()
            log.debug("File %s exists locally", self.local_filepath())
        else:
            download_ctr = 0
            while True:
                try:
                    log.debug("Start download, %s", self.url())
                    t1 = datetime.utcnow()
                    r = requests.get(self.url(), stream=True, timeout=timeout_sec)
                    t2 = datetime.utcnow()
                    self._duration = t2-t1
                    if r.status_code == 200:
                        with open(self.local_tmp_filepath(), 'wb') as f:
                            r.raw.decode_content = True
                            shutil.copyfileobj(r.raw, f)
                        if self.md5_ok():
                            shutil.move(self.local_tmp_filepath(), self.local_filepath())
                            log.info("File %s downloaded, size %s (%s), MD5 is OK", self.local_filepath(), self.file_size(), self.human_file_size())
                            self._state = DownloadOK()
                            # OK
                            break
                        else:
                            self._state = BadCRC()
                            self.local_tmp_filepath().unlink()
                            log.info("File %s downloaded but has broken CRC, file removed ", self.local_tmp_filepath())
                    else:
                        self._state = HTTPError(r.status_code)
                        log.info("HTTP error %s in download URL %s", r.status_code, self.url())
                except RequestException as rex:
                    self._state = ConnectionError(rex)
                    log.info("Failure in network connection for URL %s download", self.url())
                except Exception as ex:
                    self._state = GenericError(ex)
                    log.debug("Generic error during download of URL %s", self.url())
                finally:
                    if not self._state.ok():
                        download_ctr += 1
                        log.info("Previous download failed, it was download attempt %s", download_ctr)
                        if download_ctr > self._max_retry:
                            log.error("Max number of retry for URL %s reached, abort download", self.url())
                            break
                        wait_time = min((2**download_ctr), self._maximum_backoff)
                        log.info("Wait %s seconds before retry", wait_time)
                        time.sleep(wait_time)
        return self.local_filepath()


    def download_dir(self):
        return self._local_dir

    def md5(self):
        hash_md5 = hashlib.md5()
        with open(self.local_tmp_filepath(), "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def md5_ok(self):
        return self._info['md5'] == self.md5()
        #return self._info['md5'] == self.md5()

    def delete_local_file(self):
        self.local_filepath().unlink()

    def __str__(self):
        return "Download operation for file {} result in: {} ".format(self.filename, str(self._state))

    def __repr__(self):
        return self.__str__()

    def duration(self):
        return self._duration

    def duration_seconds(self):
        return self._duration.total_seconds()

    def bandwidth(self):
        return float(self.file_size()) / float(self._duration.total_seconds())

    def was_downloaded(self):
        return type(self._state) == DownloadOK

    def file_was_present(self):
        return type(self._state) == FileAlreadyPresent

    def transfer_error(self):
        return not self._state.ok()

    def state(self):
        return self._state



class RepoData(Package):
    def __init__(self, base_url, local_dir=tempfile.mkdtemp(prefix="condarepo", dir="/tmp/")):
        super().__init__(base_url, "repodata.json", local_dir=local_dir)

    def md5_ok(self):
        return True

    def file_exists_locally(self):
        return False


