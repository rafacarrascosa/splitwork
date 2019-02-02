import os
import tempfile


def _read(name):
    with open(name) as f:
        x = f.read()
    return x


def _write(name, content):
    with open(name, "w") as f:
        f.write(content)


class BaseTestSplitwork:
    def setUp(self):
        self._tempfiles = []

    def tearDown(self):
        names = set(x.name for x in self._tempfiles)
        for name in names:
            os.remove(name)
        for x in self._tempfiles:
            x.close()

    def tempfiles(self, n):
        handles = [tempfile.NamedTemporaryFile(delete=False) for _ in range(n)]
        names = [x.name for x in handles]
        self._tempfiles += handles
        return names

    def get_fds(self, paths, mode):
        if isinstance(paths, str):
            paths = [paths]
        handles = [open(name, mode) for name in paths]
        self._tempfiles += handles
        return [x.fileno() for x in handles]
