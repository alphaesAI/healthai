import inspect

from types import FunctionType, MethodType

import mcpadapt.core

from mcpadapt.smolagents_adapter import SmolAgentsAdapter
from smolagents import PythonInterpreterTool, Tool, tool as CreateTool, VisitWebpageTool, WebSearchTool
from transformers.utils import chat_template_utils, TypeHintParsingException                #chat template utils: parse docstrings

from ...embeddings import Embeddings
from .embeddings import EmbeddingsTool
from .function import FunctionTool


class ToolFactory:
    DEFAULTS = {"python": PythonInterpreterTool(), "websearch": WebSearchTool(), "webview": VisitWebpageTool()}

    @staticmethod
    def create(config):
        """
        this method iterates of the tools configuration
        """

        tools = []
        for tool in config.pop("tools", []):        #[] -> safety fallback
            # if it's not already a tool object & it's either function or method OR just anything callable
            if not isinstance(tool, Tool) and (isinstance(tool, (FunctionTool, MethodType)) or hasattr(tool, "__call__")):
                tool = ToolFactory.createtool(tool)
            
            elif isinstance(tool, dict):
                target = tool.get("target")

                tool = (
                    EmbeddingsTool(tool)
                    if isinstance(target, Embeddings) or any(x in tool for x in ["container", "path"])
                    else ToolFactory.createtool(target, tool)
                )

            elif isinstance(tool, str) and tool in ToolFactory.DEFAULTS:
                tool = ToolFactory.DEFAULTS[tool]

            elif isinstance(tool, str) and tool.startswith("http"):
                tools.extend(mcpadapt.cor.MCPAdapt({"url": tool}, SmolAgentsAdapter()).tools())
                tool = None
            
            if tool:
                tools.append(tool)

        return tools
    
    @staticmethod
    def createtool(target, config=None):
        try:
            # create tools with it's typehints
            return CreateTool(target)
        except (TypeHintParsingException, TypeError):
            return ToolFactory.fromdocs(target, config if config else {})

    @staticmethod
    def fromdocs(target, config):
        #if it's a normal function or method use it's name. if it doesn't have a call method but treated like a normal function use it's name 
        # else have call and it's a callable class instance use else.
        name = target.__name__ if isinstance(target, (FunctionType, MethodType)) or not hasattr(target, "__call__") else target.__class__.__name__  #pick a readable name
        target = target if isinstance(target, (FunctionType, MethodType)) or not hasattr(target, "__call__") else target.__call__                   # decides what we actually call later

        doc = inspect.getdoc(target)
        description, parameters, _ = chat_template_utils.parse_google_format_docstring(doc.strip()) if doc else (None, {}, None)

        signature = inspect.signature(target)       #shape of the function
        inputs = {}             #"name": {"type": "any", "description": "..."}
        for pname, param in signature.parameters.items():       #pname: key, param: value
            if param.default == inspect.Parameter.empty and pname in parameters:    #required + mentioned in docstring
                inputs[pname] = {"type": "any", "description": parameters[pname]}

        return FunctionTool(
            {
                "name": config.get("name", name.lower()),
                "description": config.get("description", description),
                "inputs": config.get("inputs", inputs),
                "target": config.get("target", target),
            }
        )
