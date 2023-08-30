from platform import node
from typing import List

from loguru import logger

from batchflow.decorators import log_time

from .graph import GraphEngine
from .node import ConsumerNode, ProcessorNode, ProducerNode
from enum import Enum


class FLOW_STATUS(Enum):
    IDLE = 0
    RUNNING = 1
    COMPLETE = 2
    FAIL = 3


def _task_data_from_node_tsort(tsort_l):
    processer = []
    producers = []
    consumers = []

    for i in range(len(tsort_l)):
        node = tsort_l[i]
        if isinstance(node, ProducerNode):
            task_data = (node, i, None, i >= (len(tsort_l) - 1))
            producers.append(task_data)

        elif isinstance(node, ProcessorNode):
            task_data = (node, i, i - 1, i >= (len(tsort_l) - 1))
            processer.append(task_data)
        elif isinstance(node, ConsumerNode):
            task_data = (node, i, i - 1, i >= (len(tsort_l) - 1))
            consumers.append(task_data)
        else:
            raise ValueError("node is not of one of the valid types")

    return producers, processer, consumers


class Flow:
    def __init__(
        self,
        producers: List[ProducerNode],
        consumers: List[ConsumerNode],
        batch_size: int = 1,
    ) -> None:
        self._graph_engine = GraphEngine(producers, consumers)
        self.batch_size = batch_size
        self._status = FLOW_STATUS.IDLE
        self._message = ""
        self._exception = None
        self.producers = []
        self.processors = []
        self.consumers = []

    # TODO: 1. Use graphlib

    @property
    def status(self):
        return self._status, self._message, self._exception

    @staticmethod
    def _open_nodes(nodes, batch_size):
        for node_data in nodes:
            node = node_data[0]
            node.batch_size = batch_size
            node.open()

    @staticmethod
    def _close_nodes(nodes):
        for node_data in nodes:
            node = node_data[0]
            node.close()

    def open(self):
        # open all tasks
        self._open_nodes(self.producers, self.batch_size)
        self._open_nodes(self.processors, self.batch_size)
        self._open_nodes(self.consumers, self.batch_size)

    def close(self):
        self._close_nodes(self.producers)
        self._close_nodes(self.processors)
        self._close_nodes(self.consumers)

    def setup(self):
        tsort = self._graph_engine.topological_sort()
        o = _task_data_from_node_tsort(tsort)
        producers: List[ProducerNode] = o[0]
        processer: List[ProcessorNode] = o[1]
        consumers: List[ConsumerNode] = o[2]

        self.producers = producers
        self.processors = processer
        self.consumers = consumers

    @log_time
    def run(self, manual=False):
        logger.info("Running Flow...\n\n")
        logger.info(f"Batch size={self.batch_size}")
        
        if not manual:
            self.setup()
            if len(self.producers) > 1:
                logger.warning(
                    f"{len(self.producers)} producers found, flow will end if any of the producer ends! Make sure they produce same no. of units"
                )
            self.open()

        last_ctx = None
        self._status = FLOW_STATUS.RUNNING
        try:
            if self.batch_size == 1:

                # process the flow sequentially
                while True:

                    # get the producers output
                    ctx = {}
                    try:
                        for prod_data in self.producers:
                            prod = prod_data[0]
                            out = prod.next()
                            ctx = {**out, **ctx}
                    except StopIteration:
                        break

                    # process the unit
                    for proc_data in self.processors:
                        proc = proc_data[0]
                        ctx = proc.process(ctx)

                    # consume the unit
                    for con_data in self.consumers:
                        con = con_data[0]
                        con.consume(ctx)

                    last_ctx = ctx
            else:
                # process the flow sequentially
                while True:

                    # get the self.producers output
                    ctx = {}
                    try:
                        for prod_data in self.producers:
                            prod = prod_data[0]
                            out = prod.next_batch()
                            ctx = {**out}
                    except StopIteration:
                        break

                    # process the unit
                    for proc_data in self.processors:
                        proc = proc_data[0]
                        ctx = proc.process_batch(ctx)

                    # consume the unit
                    for con_data in self.consumers:
                        con = con_data[0]
                        con.consume_batch(ctx)
                    last_ctx = ctx
            self._status = FLOW_STATUS.COMPLETE
        except Exception as e:
            import traceback

            error_message = traceback.format_exc()
            logger.error(error_message)
            self._status = FLOW_STATUS.FAIL
            self._message = error_message
            self._exception = e
        finally:
            # close all tasks
            if not manual:
                self.close()
            logger.info("Flow Ended Sucessfully")

        return last_ctx
