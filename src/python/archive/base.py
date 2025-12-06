import os

from tempfile import TemporaryDirectory

from .tar import Tar
from .zip import Zip

class Archive:
    def __init__(self, directory=None):
        self.directory = directory

    def isarchive(self, path):
        return path and any(path.lower().endswith(extension) for extension in [".tar.bz2", ".tar.gz", ".tar.xz", ".zip"])
    
    def path(self):
        if not self.directory:
            self.directory = TemporaryDirectory()

        return self.directory.name if isinstance(self.directory, TemporaryDirectory) else self.directory
    
    def load(self, path, compression=None):
        compress = self.create(path, compression)
        compress.unpack(path, self.path())

    def save(self, path, compression=None):
        output = os.path.dirname(path)
        if output:
            os.makedirs(output, exist_ok=True)

        compress = self.create(path, compression)
        compress.pack(self.path(), path)

    def create(self, path, compression):
        compression = compression if compression else path.lower().split(".")[-1]

        return Zip() if compression == "zip" else Tar()
    