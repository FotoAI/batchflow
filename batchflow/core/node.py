from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import sys
from tkinter import EXCEPTION
import loguru

from batchflow.storage.base import BaseStorage

logger = loguru.logger

import pickle
import numpy as np
from batchflow.constants import GPU, CPU, DEVICE_TYPES
import boto3
import os
from batchflow.constants import BATCH, REALTIME, MODE
from typing import Any, List


class Node:
    """
    Represents a computational node in the graph. It is also a callable object. \
        It can be call with the list of parents on which it depends.
    """

    def __init__(self, name=None, mode=BATCH):
        if name is None:
            name = self.__class__.__name__

        self._name = name
        self._parents = None
        self._id = id(self)
        self._children = set()
        self._is_part_of_taskmodule_node = False
        self.state_attributes = []
        self.progress = 0
        self.state_attributes.extend(["_name", "progress"])
        self._logger = self._configure_logger()
        self._logger.debug(f"Created Node with id {self._id}")
        self._batch_size = 1
        if mode in MODE:
            self.mode = MODE
        else:
            raise EXCEPTION(f"execution mode: {mode} should be one of {MODE}")
        # self._configure_execution_mode()

    def _configure_execution_mode(self):
        raise NotImplemented("Implement in this Particular Node")

    def _configure_logger(self):
        # logger = logging.getLogger(f"{self.__repr__()}_{self._id}")
        # logger.setLevel(LOGGING_LEVEL)
        # ch = logging.StreamHandler()
        # ch.setLevel(LOGGING_LEVEL)
        # formatter = logging.Formatter(
        #     "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        # )
        # ch.setFormatter(formatter)
        # # logger.addHandler(ch)
        return loguru.logger
        # return logger

    def __repr__(self):
        if not self._name:
            return self.__class__.__name__
        else:
            return self._name

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return self._id

    def register_state_attr(self, *attributes):
        """
        Register the variables in states that are responsible to restore the
        producer state in case of failure
        """
        for attribute in attributes:
            if attribute not in self.state_attributes:
                self.state_attributes.append(attribute)

    # def get_current_state(self):
    #     if Config.SAVE_STATES and self.progress % Config.STATES_INTERVAL == 0:
    #         state = pickle.dumps(self)
    #         return state
    #     else:
    #         self._logger.debug("No states")

    def __getstate__(self):
        state_dict = {}
        for k in self.state_attributes:
            state_dict[k] = getattr(self, k)
        return state_dict

    def __setstate__(self, state):
        self.__dict__.update(state)

    def restore(self):
        """
        Override this method to write custom function for restoring the state if necessary
        """
        pass

    def open(self):
        """
        This method is called by the task runner before doing any consuming,
        processing or producing.  Should be used to open any resources
        that will be needed during the life of the task, such as opening files,
        tensorflow sessions, etc.
        """
        pass

    def close(self):
        """
        This method is called by the task running after finishing doing all
        consuming, processing or producing because of and end signal receival.
        Should be used to close any resources
        that were opened by the open() method, such as files,
        tensorflow sessions, etc.
        """
        pass

    @property
    def id(self):
        """
        The id of the node.  In this case the id of the node is produced by calling
        ``id(self)``.
        """
        # ****IMPORTANT********
        # Must not be changed. Computing the id at call time will likely introduce errors
        # because the Python built-in `id` function might return different ids for the same
        # graph node if called from different processes.  So is better to compute it once
        # from the process where all constructors are called, and then save it for later.
        return self._id

    def __call__(self, *parents):
        if self._parents is not None:
            raise RuntimeError(
                "This method has already been called. It can only be called once."
            )
        if self._is_part_of_taskmodule_node:
            raise RuntimeError(
                "Cannot make a node that belongs to a TaskModuleNode to be"
                " a child of another node in the graph. Use the TaskModuleNode for the edge"
                " if possible."
            )
        for parent in parents:
            if parent._is_part_of_taskmodule_node:
                raise RuntimeError(
                    "Cannot make a node that belongs to a TaskModuleNode to be"
                    " a parent of another node. Use the TaskModuleNode as parent"
                    " if possible."
                )
        self._parents = list()
        for parent in parents:
            # assert isinstance(parent, Node) and not isinstance(parent, Leaf), '%s is not a non-leaf node' % str(parent)
            assert isinstance(parent, Node), "%s is not a node" % str(parent)
            self._parents.append(parent)
            parent.add_child(self)
        return self

    def add_child(self, child):
        """
        Adds child to the set of childs that depend on it.
        """
        self._children.add(child)

    def remove_child(self, child):
        self._children.remove(child)

    @property
    def parents(self):
        """
        Returns a list with the parent nodes
        """
        if self._parents is None:
            return None
        return list(self._parents)

    @property
    def children(self):
        """
        Returns a set of the child nodes
        """
        return set(self._children)

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, batch_size):
        self._batch_size = batch_size


class Leaf(Node):
    """
    Node with no children.
    """

    def __init__(self, *args, **kwargs):
        self._children = None
        super(Leaf, self).__init__(*args, **kwargs)


class ConsumerNode(Leaf):
    """
    - Arguments:
        - metadata (boolean): By default is False. If True, instead of receiving \
            output of parent nodes, receives metadata produced by parent nodes.
    """

    def __init__(self, metadata=False, **kwargs):
        self._metadata = metadata
        super(ConsumerNode, self).__init__(**kwargs)

    @property
    def metadata(self):
        return self._metadata

    def consume(self, item):
        """
        Method definition that needs to be implemented by subclasses.

        - Arguments:
            - item: the item being received as input (or consumed).
        """
        raise NotImplementedError(
            "consume function needs to be implemented\
                            by subclass"
        )


class AsyncConsumerNode(Leaf):
    """
    - Arguments:
        - metadata (boolean): By default is False. If True, instead of receiving \
            output of parent nodes, receives metadata produced by parent nodes.
    """

    def __init__(self, nb_concurrent_tasks: int = 4, metadata=False, **kwargs):
        self._metadata = metadata
        self._nb_concurrent_tasks = nb_concurrent_tasks
        self.list_tasks = []
        self.debug_list_tasks = []
        self.last_task = False
        self.callback = None
        super(AsyncConsumerNode, self).__init__(**kwargs)

    @property
    def metadata(self):
        return self._metadata

    def set_loop(self, loop):
        self.loop = loop

    def debug_frame_num(self):
        n = []
        for d in self.debug_list_tasks:
            n.append(d["meta_data"]["frame_number"])
        n = np.array(n)
        self._logger.info(
            f"Debug Duplicate: Progress: {self.progress} Request: data: {len(self.debug_list_tasks)}"
            f" unique: {len(np.unique(n))}"
        )

    def debug_response(self, response):
        s = 0
        d = 0
        for r in response:
            try:
                if r.status == 200:
                    s += 1
                elif r.status == 400:
                    d += 1
            except:
                pass
        self._logger.info(
            f"Debug Duplicate: Progress: {self.progress} Response: success: {str(s)}"
            f" duplicate: {str(d)}"
        )

    async def _gather(self):
        """
        gather all the tasks in the list_tasks
        """
        asyncio.set_event_loop(self.loop)
        output = await asyncio.gather(*self.list_tasks)
        return output

    def set_callback(self, callback):
        self.callback = callback

    async def consume(self, item):
        """
        Method definition for a async function needs to be implemented by subclass.

        - Arguments:
            inp : object or list of objects being received for processing \
                from parent nodes.
        - Returns:
            - processed data
        """
        raise NotImplementedError()

    def is_pending_tasks(self):
        """
        Return True if there are tasks remaining in the list_tasks
        """
        return len(self.list_tasks) > 0

    def _submit_task(self, *args):
        self.list_tasks.append(self.consume(*args))
        self.debug_list_tasks.append(*args)

    async def consume_all(self):
        self.debug_frame_num()
        response = await self._gather()
        self.debug_response(response)
        self.list_tasks = []
        self.debug_list_tasks = []
        self.callback(response)

    async def consume_async(self, *args):
        """
        Method definition that needs to be implemented by subclasses.

        - Arguments:
            - item: the item being received as input (or consumed).
        """
        self._submit_task(*args)
        if len(self.list_tasks) >= self._nb_concurrent_tasks:
            self.debug_frame_num()
            response = await self._gather()
            self.debug_response(response)
            self.list_tasks = []
            self.debug_list_tasks = []
            self.callback(response)


class ProcessorNode(Node):
    def __init__(self, nb_tasks: int = 1, device_type=CPU, mode=BATCH, *args, **kwargs):
        self._nb_tasks = nb_tasks
        self._meta_data = None
        if device_type not in DEVICE_TYPES:
            raise ValueError("Device is not one of {}".format(",".join(DEVICE_TYPES)))
        self._device_type = device_type
        super(ProcessorNode, self).__init__(mode=mode, *args, **kwargs)

    # def _configure_execution_mode(self):
    #     if self.mode == BATCH:
    #         self._process_single_batch = self.process
    #         self.process = self.process_batch
    #         self._logger.warning(
    #             f"""By default batch processing will call process in loop
    #             To implement more efficient batch processing override method process_batch"""
    #         )

    @property
    def meta_data(self):
        """
        Returns metadata of the processor
        """
        return self._meta_data

    @property
    def nb_tasks(self):
        """
        Returns the number of tasks to allocate to this processor
        """
        return self._nb_tasks

    @property
    def device_type(self):
        """
        Returns the preferred device type to use to run the processor's code
        """
        return self._device_type

    def change_device(self, device_type):
        if device_type not in DEVICE_TYPES:
            raise ValueError("Device is not one of {}".format(",".join(DEVICE_TYPES)))
        self._device_type = device_type

    def process(self, inp: any) -> any:
        """
        Method definition that needs to be implemented by subclasses.

        - Arguments:
            - inp: object or list of objects being received for processing \
                from parent nodes.
        
        - Returns:
            - the output being consumed by child nodes.
        """
        raise NotImplementedError(
            "process function needs to be implemented\
                            by subclass"
        )

    def process_batch(self, inp: List[Any]) -> Any:
        raise NotImplementedError(
            "process_batch function needs to be implemented\
                            by subclass"
        )


class ProducerNode(Node):
    """
    The `producer node` does not receive input, and produces input. \
        Each time the ``next()`` method is called, it produces a new input.
    
    It would have been more natural to implement the ``ProducerNode`` as a generator, \
        but generators cannot be pickled, and hence you cannot easily work with generators \
        in a multiprocessing setting.
    """

    def __init__(self, *args, **kwargs):
        super(ProducerNode, self).__init__(*args, **kwargs)

    def restore(self):
        p = 0
        while p != self.progress:
            self.next()
            p += 1

    def next(self) -> any:
        """
        Returns next produced element.

        Raises ``StopIteration`` after the last element has been produced
        and a call to self.next happens.
        """
        raise NotImplementedError("Method needs to be implemented by subclass")

    def next_batch(self)-> any:
        """
        Returns batch of next elements
        Raises ``StopIteration`` after the last element has been produced
        and a call to self.next happens.
        """
        raise NotImplementedError("Method needs to be implemented by subclass")
        
