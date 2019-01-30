import os
import logging
from pathlib import Path

log = logging.getLogger("condarepo")


class PidFile():
    def __init__(self, filepath):
        self._filepath = Path(filepath)

    def can_start(self):
        if self._filepath.exists():
            log.fatal("Found previous pid file %s, something was wrong during last run", self._filepath)
            return False
        else:
            self._filepath.write_text(str(os.getpid()))
            log.info("Pid file %s created", self._filepath)
            return True

    def cleanup(self):
        self._filepath.unlink()
        log.info("Pid file %s removed", self._filepath)
