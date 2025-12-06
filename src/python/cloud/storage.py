import os

try:
    from libcloud.storage.providers import get_driver, DRIVERS
    from libcloud.storage.types import ContainerDoesNotExistError, ObjectDoesNotExistError

    LIBCLOUD = True
except ImportError:
    LIBCLOUD, DRIVERS = False, None

from .base import Cloud

class ObjectStorage(Cloud):
    @staticmethod
    def isprovider(provider):
        return LIBCLOUD and provider and provider.lower() in [x.lower() for x in DRIVERS]
    
    def __init__(self, config):         #cloud configurations
        super().__init__(config)

        if not LIBCLOUD:
            raise ImportError('Cloud object storage is not available - install "libcloud" ')
        
        driver = get_driver(config["provider"])

        self.client = driver(
            config.get("key", os.environ.get("ACCESS_KEY")),
            config.get("secret", os.environ.get("ACCESS_SECRET")),
            **{field: config.get(field) for field in ["host", "port", "region", "token"] if config.get(field)},
        )
    
    def metadata(self, path=None):
        try:
            if self.isarchive(path):
                return self.client.get_object(self.config["container"], self.objectname(path))
            
            return self.client.get_container(self.config["container"])
        except (ContainerDoesNotExistError, ObjectDoesNotExistError):
            return None
        
    def load(self, path=None):
        if self.isarchive(path):
            obj = self.client.get_object(self.config["container"], self.objectname(path))

            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            obj.download(path, overwrite_existing=True)

        else:
            container = self.client.get_container(self.config["container"])
            for obj in container.list_objects(prefix=self.config.get("prefix")):
                localpath = os.path.join(path, obj.name)
                directory = os.path.dirname(localpath)

                os.makedirs(directory, exist_ok=True)

                obj.download(localpath, overwrite_existing=True)

        return path
    
    def save(self, path):
        try:
            container = self.client.get_container(self.config["container"])
        except ContainerDoesNotExistError:
            container = self.client.create_container(self.config["container"])

            for f in self.listfiles(path):
                with open(f, "rb") as iterator:
                    self.client.upload_object_via_stream(iterator=iterator, container=container, object_name=self.objectname(f))

    def objectname(self, name):
        name = os.path.basename(name)

        prefix = self.config.get("prefix")