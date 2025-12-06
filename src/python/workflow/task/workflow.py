from .base import Task


class WorkflowTask(Task):
    def process(self, action, inputs):
        return list(super().process(action, inputs))