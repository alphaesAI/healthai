import hashlib
import os

from fastapi import Header, HTTPException

class Authorization:
    def __init__(self, token=None):
        """
        token: hashed string 
        """
        self.token = token if token else os.environ.get("TOKEN")
    
    def __call__(self, authorization: str = Header(default=None)):
        print("call method called")
        print("authorization header received:", authorization)
        if not authorization or self.token != self.digest(authorization):
            raise HTTPException(status_code=401, detail="Invalid Authorization Token")
    
    def digest(self, authorization):
        print("digest called")
        prefix = "Bearer "
        token = authorization[len(prefix) :] if authorization.startswith(prefix) else authorization

        return hashlib.sha256(token.encode("utf-8")).hexdigest()