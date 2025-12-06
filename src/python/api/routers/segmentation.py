from typing import List

from fastapi import APIRouter, Body

from .. import application
from ..route import EncodingAPIRoute

router = APIRouter(route_class=EncodingAPIRoute)

@router.get("/segment")
def segment(text: str):
    return application.get().pipeline("segmentation", (text,))

@router.post("/batchsegment")
def batchsegment(texts: List[str] = Body(...)):
    return application.get().pipeline("segmentation", (texts,))