import functools

from ...util import Resolver

class TaskFactory:
    @staticmethod
    def get(task):
        if "." not in task:
            task = ".".join(__name__.split(".")[:-1]) + "." + task.capitalize() + "Task"

        return Resolver()(task)
    
    @staticmethod
    def create(config, task):
        if "args" in config:
            args = config.pop("args")
            action = config["action"]
            if action:
                if isinstance(action, list):
                    config["action"] = [Partial.create(a, args[i]) for i, a in enumerate(action)]
                else:
                    config["action"] = lambda x: action(x, **args) if isinstance(args, dict) else action(x, *args)

        return TaskFactory.get(task)(**config)
    

class Partial(functools.partial):
    @staticmethod
    def create(action, args):
        return Partial(action, **args) if isinstance(args, dict) else Partial(action, *args) if args else Partial(action)
    
    def __call__(self, *args, **kwargs):
        kw = self.keywords.copy()
        kw.update(kwargs)

        return self.fun(*(args + self.args), **kw)