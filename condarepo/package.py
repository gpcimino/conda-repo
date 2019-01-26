import tempfile
from pathlib import Path
from furl import furl
import shutil
import logging
import hashlib
import requests
import backoff

log = logging.getLogger("condarepo")

class Package():
    def __init__(self, base_url, filename, local_dir=tempfile.mkdtemp(prefix="condarepo", dir="/tmp/"), **kwargs):
        self._base_url = furl(base_url)
        self.filename = filename
        self._info = kwargs
        self._local_dir = Path(local_dir)
        self._state = 'to be checked'

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
        if self.file_exists_locally():
            self._state = 'exists locally'
            log.debug("File %s exists locally", self.local_filepath())
        else:
            try:
                log.info("Start download, %s", self.url())
                r = requests.get(self.url(), stream=True, timeout=timeout_sec)
                if r.status_code == 200:
                    with open(self.local_tmp_filepath(), 'wb') as f:
                        r.raw.decode_content = True
                        shutil.copyfileobj(r.raw, f)
                    if self.md5_ok():
                        shutil.move(self.local_tmp_filepath(), self.local_filepath())
                        log.info("File %s downloaded, size %s, MD5 is OK", self.local_filepath(), self.file_size())
                        self._state = 'downloaded'
                        return self.local_filepath()
                    else:
                        self._state = 'bad crc'
                        raise Exception("Local file has invalild CRC")
                else:
                    log.error("HTTP error %s in download URL %s", r.status_code, self.url())
                    raise Exception("HTTP error %s", r.status_code)
            except Exception as ex:
                self._state = 'download failure'
                msg = "Failure in HTTP download for {}".format(self.url())
                log.exception(msg)
                raise Exception(msg) from ex

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
        if self._state == "downloaded":
            return "{} {} {} bytes".format(self.filename, self._state, self.file_size())
        else:
            return "{} {}".format(self._state, self.filename)
    def __repr__(self):
        return self.__str__()



class RepoData(Package):
    def __init__(self, base_url, architecture, local_dir=tempfile.mkdtemp(prefix="condarepo", dir="/tmp/")):
        super().__init__(base_url, "repodata.json", local_dir=local_dir,  **{'subdir': architecture})

    def md5_ok(self):
        return True

    def file_exists_locally(self):
        return False


