from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from esai.chroma.base import Chroma

router = APIRouter()
chroma = Chroma()

class CreateCollectionRequest(BaseModel):
    name: str

class AddDocsRequest(BaseModel):
    collection_name: str
    ids: List[str]
    documents: List[str]

class UpdateDocsRequest(BaseModel):
    collection_name: str
    ids: List[str]
    documents: Optional[List[str]] = None

class DeleteDocsRequest(BaseModel):
    collection_name: str
    ids: List[str]

@router.post("/collections")
def create_collection(req: CreateCollectionRequest):
    collection = chroma.create_collection(req.name)
    return {"message": f"Collection '{req.name}' created succesfully", "collection": collection.name}

@router.get("/list_collections")
def list_collections():
    collections = chroma.list_collections()
    return {"collections": [c.name for c in collections]}

@router.post("/add_documents")
def add_documents(req: AddDocsRequest):
    try:
        chroma.add_documents(req.collection_name, req.ids, req.documents)
        return {"message": "Documents added successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@router.put("/update_documents")
def update_documents(req: UpdateDocsRequest):
    try:
        chroma.update_documents(req.collection_name, req.ids, req.documents)
        return {"message": "Documents updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@router.delete("/delete_documents")
def delete_documents(req: DeleteDocsRequest):
    try:
        chroma.delete_documents(req.collection_name, req.ids)
        return {"message": "Documents deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@router.get("/view_documents")
def get_documents(collection_name: str):
    docs = chroma.get_documents(collection_name)
    return {"collection": collection_name, 
            "documents": list(docs.get("documents", [])),
            "ids": list(docs.get("ids", []))
            }