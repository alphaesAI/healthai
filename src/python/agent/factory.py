from smolagents import CodeAgent, ToolCallingAgent

from .model import PipelineModel
from .tool import ToolFactory

class ProcessFactory:
    @staticmethod
    def create(config):
        constructor = ToolCallingAgent
        method = config.pop("method", None)
        if method == "code":
            constructor = CodeAgent

        model = config.pop("model", config.pop("llm", None))        #pick the value of the model or llm
        model = PipelineModel(**model) if isinstance(model, dict) else PipelineModel(model)

        return constructor(tools=ToolFactory.create(config), model=model, **config)