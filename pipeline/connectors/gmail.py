import os
import pickle
from typing import Optional
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from .base import BaseConnector


class GmailConnector(BaseConnector):
    """Gmail API connector for fetching emails and attachments."""

    def __init__(self, config: dict):
        """
        Args:
            config: dictionary containing keys
                - credentials_path
                - token_path
                - scopes (optional, defaults to read-only scope)
                - api_version (optional, defaults to 'v1')
                - auth_port (optional, defaults to 0)
        """

        # Pass config to BaseConnector
        super().__init__(config)

        # Local config
        self.service: Optional[Resource] = None
        self.credentials_path = config.get("credentials_path")
        self.token_path = config.get("token_path")
        self.scopes = config.get("scopes", os.getenv('GMAIL_SCOPES', "https://www.googleapis.com/auth/gmail.readonly").split(','))
        self.api_version = config.get("api_version", os.getenv('GMAIL_API_VERSION', 'v1'))
        self.auth_port = int(config.get("auth_port", os.getenv('GMAIL_AUTH_PORT', '0')))

    def connect(self) -> None:
        """Authenticate and build Gmail service client."""
        creds = None

        if os.path.exists(self.token_path):
            with open(self.token_path, "rb") as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.scopes
                )
                creds = flow.run_local_server(port=self.auth_port)

            with open(self.token_path, "wb") as token:
                pickle.dump(creds, token)

        self.service = build("gmail", self.api_version, credentials=creds)

    def disconnect(self) -> None:
        self.service = None

    def test_connection(self) -> bool:
        """Test Gmail connection."""
        try:
            if not self.service:
                return False
            profile = self.service.users().getProfile(userId="me").execute()
            return profile is not None
        except Exception:
            return False

    def get_connection_info(self):
        """Required abstract method."""
        return {
            "credentials_path": self.credentials_path,
            "token_path": self.token_path,
        }
