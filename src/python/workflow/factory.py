from .base import Workflow
from .task import TaskFactory

class WorkflowFactory:
    @staticmethod
    def create(config, name):
        tasks = []
        for tconfig in config["tasks"]:
            task = tconfig.pop("task") if "task" in tconfig else ""
            tasks.append(TaskFactory.create(tconfig, task))

        config["tasks"] = tasks

        return Workflow(**config, name=name)