# extractors/registry.py

from .email import GmailExtractor
from .postgres import PostgresExtractor
from .factory import ExtractorFactory

# register extractors
ExtractorFactory.register("gmail", GmailExtractor)
