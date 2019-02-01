import tempfile
from pathlib import Path
from furl import furl
import shutil
import logging
from datetime import datetime
import hashlib

import requests
from requests.exceptions import RequestException
import backoff

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
    def __init__(self, base_url, filename, local_dir=tempfile.mkdtemp(prefix="condarepo", dir="/tmp/"), **kwargs):
        self._base_url = furl(base_url)
        self.filename = filename
        self._info = kwargs
        self._local_dir = Path(local_dir)
        self._state = NotStarted()
        self._duration = None

    def url(self):
        b = self._base_url.copy().join(self._info['subdir'] + "/")
        return str(b.copy().join(self.filename))

    def local_filepath(self):
        return self._local_dir / self.filename

    def local_tmp_filepath(self):
        return self.local_filepath().with_suffix(".tmp-download")

    def file_exists_locally(self):
        return self.local_filepath().exists()

    def file_size(self):
        return self.local_filepath().stat().st_size

    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, Exception), max_tries=5)
    def download(self, timeout_sec=10):
        log.debug("%s", self.local_filepath())
        if self.file_exists_locally():
            self._state = FileAlreadyPresent()
            log.debug("File %s exists locally", self.local_filepath())
        else:
            try:
                log.info("Start download, %s", self.url())
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
                        log.info("File %s downloaded, size %s, MD5 is OK", self.local_filepath(), self.file_size())
                        self._state = DownloadOK()
                        return self.local_filepath()
                    else:
                        self._state = BadCRC()
                        raise Exception("Local file has invalid CRC")
                else:
                    self._state = HTTPError(r.status_code)
                    log.error("HTTP error %s in download URL %s", r.status_code, self.url())
                    raise Exception("HTTP error %s", r.status_code)
            except RequestException as rex:
                self._state = ConnectionError(rex)
                log.exception("Failure in HTTP download for %s download", self.url())
            except Exception as ex:
                self._state = GenericError(ex)
                log.exception("Generic error for %s download", self.url())

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

    def delete_local_file(self):
        self.local_filepath().unlink()

    def __str__(self):
        return "Download operation for file {} result in: {} ".format(self.filename, str(self._state))

    def __repr__(self):
        return self.__str__()

    def duration(self):
        return self._duration

    def bandwidth(self):
        return float(self.file_size()) / float(self._duration.total_seconds())

    def was_downloaded(self):
        return self._state == 'downloaded'

    def file_was_present(self):
        self._state == 'exists locally'

    def state(self):
        return self._state



class RepoData(Package):
    def __init__(self, base_url, architecture, local_dir=tempfile.mkdtemp(prefix="condarepo", dir="/tmp/")):
        super().__init__(base_url, "repodata.json", local_dir=local_dir,  **{'subdir': architecture})

    def md5_ok(self):
        return True

    def file_exists_locally(self):
        return False


