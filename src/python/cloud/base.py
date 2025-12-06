import os
from ..archive import ArchiveFactory

class Cloud:
    def __init__(self, config):
        self.config = config

    def exists(self, path=None):
        return self.metadata(path) is not None
    
    def metadata(self, path=None):
        raise NotImplementedError
    
    def load(self, path=None):
        raise NotImplementedError
    
    def save(self, path):
        raise NotImplementedError
    
    def isarchive(self, path):
        return ArchiveFactory.create().isarchive(path)
    
    def listflies(self, path):
        if os.path.isdir(path):
            return [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        
        return path