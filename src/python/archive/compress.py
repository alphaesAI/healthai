import os

class Compress:
    def pack(self, path, output):
        raise NotImplementedError
    
    def unpack(self, path, output):
        raise NotImplementedError
    
    def validate(self, directory, path):
        directory = os.path.abspath(directory)
        path = os.path.abspath(path)
        prefix = os.path.commonprefix([directory, path])

        return prefix == directory