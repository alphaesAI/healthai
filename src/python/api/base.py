import json

from ..app import Application

class API(Application):
    def __init__(self, config, loaddata=True):
        super().__init__(config, loaddata)
