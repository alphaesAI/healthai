import os
import inspect
import sys

from fastapi import FastAPI, APIRouter, Depends
from fastapi_mcp import FastApiMCP
from httpx import AsyncClient

from ..app import Application

from .factory import APIFactory
from .base import API
from .authorization import Authorization

def get():
    return INSTANCE 

def create():
    dependencies = []
    token = os.environ.get("TOKEN")
    if token:
        dependencies.append(Depends(Authorization(token)))
    
    return FastAPI(lifespan=lifespan, dependencies=dependencies if dependencies else None)

def lifespan(application):

    global INSTANCE

    config = Application.read(os.environ.get("CONFIG"))

    api = os.environ.get("API_CLASS")
    INSTANCE = APIFactory.create(config, api) if api else API(config)

    routers = apirouters()

    for name, router in routers.items():
        if name in config or name == "chroma":
            application.include_router(router)

    if config.get("mcp"):
        mcp = FastApiMCP(application, http_client=AsyncClient(timeout=100))
        mcp.mount()

        @application.get("/mcp/tools")
        async def list_tools():
            return mcp.list_tools()

    yield

def apirouters():
    api = sys.modules[".".join(__name__.split(".")[:-1])]

    available = {}
    for name, rclass in inspect.getmembers(api, inspect.ismodule):
        if hasattr(rclass, "router") and isinstance(rclass.router, APIRouter):
            available[name.lower()] = rclass.router

    return available

def start():
    list(lifespan(app))

app, INSTANCE = create(), None