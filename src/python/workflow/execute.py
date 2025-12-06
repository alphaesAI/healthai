from multiprocessing.pool import Pool, ThreadPool   # Pool - collection of thread/process - subprocess, ThreadPool - used for multithreading, share global memory

import torch.multiprocessing                        # safe for gpu tasks, separate memory

class Execute:
    def __init__(self, workers=None):
        self.workers = None                         # how many parallel workers we want
        self.thread = None     
        self.process = None

    def __del__(self):                              # used to clean up the object
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, etype, value, traceback):
        """
        ensures cleanup at the end of the context in with block.

        etype: exception type
        value: actual error message
        traceback: tracing where the error happened
        """
        self.close()

    def run(self, method, function, args):
        """
        method: thread/process/None, if None, execute sequentially
        function: function we need to run
        args: list of tuples (tasks) 

        if add(a,b) then function is add args are [(a,b)]
        """
        if method and len(args) > 1:
            pool = self.pool(method)
            if pool:
                return pool.starmap(function, args, 1)
            
        return [function(*arg) for arg in args]

    def pool(self, method):
        if method == "thread":
            if not self.thread:
                self.thread = ThreadPool(self.workers)
            
            return self.thread
        
        if method == "process":
            if not self.process:
                self.process = Pool(self.workers, context=torch.multiprocessing.get_context("spawn"))
                
            return self.process
        
        return None

    def close(self):
        if hasattr(self, "thread") and self.thread:
            self.thread.close()
            self.thread.join()
            self.thread = None

        if hasattr(self, "process") and self.process:
            self.process.close()
            self.process.join()
            self.process = None