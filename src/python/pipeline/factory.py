import inspect
import sys
import types

from ..util import Resolver
from .base import Pipeline

class PipelineFactory:
    @staticmethod
    def get(pipeline):
        if "." not in pipeline:
            return PipelineFactory.list()[pipeline]
        
        return Resolver()(pipeline)
    
    # pipeline = 'tabular'
    # if . not in pipelin = 'esai.tabular'
    
    @staticmethod
    def create(config, pipeline):
        pipeline = PipelineFactory.get(pipeline)

        return pipeline if isinstance(pipeline, types.FunctionType) else pipeline(**config)
    

    
    @staticmethod
    def list():
        pipelines = {}

        pipeline = sys.modules[".".join(__name__.split(".")[:-1])]         

        for x in inspect.getmembers(pipeline, inspect.isclass):
            if issubclass(x[1], Pipeline) and [y for y, _ in inspect.getmembers(x[1], inspect.isfunction) if y == "__call__"]:
                pipelines[x[0].lower()] = x[1]
            
        return pipelines 
    
    # __name__ = esai.pipeline.factory
    # .split = ["esai", "pipeline", "factory"]
    # [:-1] = ["esai", "pipelin"]
    # join = "esai.pipeline"

    #sys.module[] = esai.pipelin.__init__.py --- base, factory, data/__init__

    # <base : <object dljfoewteit>> base - naem = x[0], oibject x[1]

    # tabular class subclass = true

    # name 
    # for name, object: y, _
    #     Tabular. lower = {tabular = class object}


