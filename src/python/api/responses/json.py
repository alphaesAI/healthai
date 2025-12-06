import base64
import json

from io import BytesIO
from typing import Any

import fastapi.responses

from PIL.Image import Image


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        """
        if condition (input, checking object type) --> returns

        if isinstance(image object, Image from PIL.Image) --> BytesIO object (image inside)

        if isinstance(BytesIO object (image inside), BytesIO) --> image bytes

        if isinstance(image bytes, bytes) --> 
        """
        if isinstance(o, Image):        
            buffered = BytesIO()
            o.save(buffered, format=o.format, quality="keep")
            o = buffered

        if isinstance(o, BytesIO):      
            print(o)
            o = o.getvalue()

        if isinstance(o, bytes): 
            """
            base64 is an enocding and decoding scheme just like a converter. 
            b64encode() converts raw bytes into base64 bytes (readable, passed across internet via http requests following ASCII characters.)
            decode("utf-8") converts them into string which is passed in json responses.
            """
            return base64.b64encode(o).decode("utf-8")

        return super().default(o)


class JSONResponse(fastapi.responses.JSONResponse):
    def render(self, content: Any) -> bytes:
        """
        json.dumps() -> python objects (dict, list, etc.,) into json string
        .encode("utf-8") -> json string into bytes
        """
        return json.dumps(content, ensure_ascii=False, allow_nan=False, indent=None, separators=(",", ":"), cls=JSONEncoder).encode("utf-8")
    