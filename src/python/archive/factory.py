from .base import Archive

class ArchiveFactory:
    @staticmethod
    def create(directory=None):
        return Archive(directory)