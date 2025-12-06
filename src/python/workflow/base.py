import logging
import time
import traceback

from datetime import datetime

try:
    from croniter import croniter

    CRONITER = True
except ImportError:
    CRONITER = False

from .execute import Execute

logger = logging.getLogger(__name__)

class Workflow:
    def __init__(self, tasks, workers=None, batch=5000, name=None, stream=None):
        """
        tasks: [Tabular(), Textractor()]
        stream: here, preprocessing method before chunking.
        """
        self.tasks = tasks
        self.workers = workers
        self.batch = batch
        self.name = name
        self.stream = stream

        self.workers = max((len(task.action) for task in self.tasks) if not self.workers else self.workers)

    def __call__(self, elements):
        """
        elements: inputs list of strings, if it's a generator or stream then it will be tuple.
        """
        with Execute(self.workers) as executor:     #Execute choose ThreadPool, ProcessPool, or sequential
            self.initialize() 

        elements = self.stream(elements) if self.stream else elements

        for batch in self.chunk(elements):
            yield from self.process(batch, executor)

        self.finalize()

    def schedule(self, cron, elements, iterations=None):
        """
        cron: cron expression as a string
        elements: inputs
        iterations: if iterations = 5, it run the workflow 5 times based on schedule. if it's none it will loop forever
        """
        if not CRONITER:
            raise ImportError("croniter is not available - intall croniter")
        
        logger.info("'%s' scheduler started with schedule %s", self.name, cron)
        
        maxiterations = iterations
        while iterations is None or iterations > 0:
            schedule = croniter(cron, datetime.now().astimezone()).get_next(datetime)
            logger.info("'%s' next run scheduled for %s", self.name, schedule.isoformat())      #isoformat() is ISO 8601 standard format
            time.sleep(schedule.timestamp() - time.time())

            try:
                for _ in self(elements):
                    pass
            except Exception:
                logger.error(traceback.format_exc())

            if iterations is not None:
                iterations -= 1

        logger.info("'%s' max iterations (%d) reached", self.name, maxiterations)

    def initialize(self):
        for task in self.tasks:
            if task.initialize:
                task.initialize()

    def chunk(self, elements):
        if hasattr(elements, "__len__") and hasattr(elements, "__getitem__"):       #__getitem__: accessing by index
            for x in range(0, len(elements), self.batch):
                yield elements[x : x + self.batch]                                  #if batch=2, len=10, then it becomes, 0: 0+2 => 0:2 => [0,1]

        else:
            batch = []
            for x in elements:
                batch.append(x)

                if len(batch) == self.batch:
                    yield batch
                    batch = []

            if batch:                               #executes if elements are not fully batched.
                yield batch

    def process(self, elements, executor):
        """
        here, elements are batches and executor is a method
        
        x: index like (0,1,2,...)
        task: callable task object
        """
        for x, task in enumerate(self.tasks):
            logger.debug("Running Task #%d", x)
            elements = task(elements, executor)     #we're passing argument to the task which class like Tabular()
        
        yield from elements                         #it returns the actual answer the user wants.

    def finalize(self):
        for task in self.tasks:
            if task.finalize:
                task.finalize()