import os
import tarfile

from .compress import Compress

class Tar(Compress):
    def pack(self, path, output):
        compression = self.compression(output)
        
        with tarfile.open(output, f"w:{compression}" if compression else "w") as tar:
            tar.add(path, arcname=".")

    def unpack(self, path, output):
        """
        path: the tar archive file you want to extract (input file)
        output: the folder you want to extract (destination folder)
        """
        compression = self.compression(path)

        with tarfile.open(path, f"r:{compression}" if compression else "r") as tar:
            for member in tar.getmembers():                                                 #member: metadata about the file
                fullpath = os.path.join(path, member.name)
                if not self.validate(path, fullpath):
                    raise IOError(f"Invalid tar entry: {member.name}")
                
            tar.extracall(output)

    def compression(self, path):
        compression = path.lower().split(".")[-1]
        return compression if compression in ("bz2", "gz", "xz") else None