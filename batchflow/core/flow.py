from platform import node
from typing import List

from loguru import logger

from batchflow.decorators import log_time

from .graph import GraphEngine
from .node import ConsumerNode, ProcessorNode, ProducerNode


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

    # TODO: 1. Use graphlib

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

    @log_time
    def run(self):
        logger.info("Running Flow...\n\n")
        logger.info(f"Batch size={self.batch_size}")
        tsort = self._graph_engine.topological_sort()
        o = _task_data_from_node_tsort(tsort)
        producers: List[ProducerNode] = o[0]
        processer: List[ProcessorNode] = o[1]
        consumers: List[ConsumerNode] = o[2]

        # open all tasks
        self._open_nodes(producers, self.batch_size)
        self._open_nodes(processer, self.batch_size)
        self._open_nodes(consumers, self.batch_size)

        if len(producers) > 1:
            logger.warning(
                f"{len(producers)} producers found, flow will end if any of the producer ends! Make sure they produce same no. of units"
            )

        last_ctx = None
        if self.batch_size == 1:

            # process the flow sequentially
            while True:

                # get the producers output
                ctx = {}
                try:
                    for prod_data in producers:
                        prod = prod_data[0]
                        out = prod.next()
                        ctx = {**out, **ctx}
                except StopIteration:
                    break

                # process the unit
                for proc_data in processer:
                    proc = proc_data[0]
                    ctx = proc.process(ctx)

                # consume the unit
                for con_data in consumers:
                    con = con_data[0]
                    con.consume(ctx)

                last_ctx = ctx
        else:
            # process the flow sequentially
            while True:

                # get the producers output
                ctx = {}
                try:
                    for prod_data in producers:
                        prod = prod_data[0]
                        out = prod.next_batch()
                        ctx = {**out}
                except StopIteration:
                    break

                # process the unit
                for proc_data in processer:
                    proc = proc_data[0]
                    ctx = proc.process_batch(ctx)

                # consume the unit
                for con_data in consumers:
                    con = con_data[0]
                    con.consume_batch(ctx)

                last_ctx = ctx

        # open all tasks
        self._close_nodes(producers)
        self._close_nodes(processer)
        self._close_nodes(consumers)
        logger.info("Flow Ended Sucessfully")

        return last_ctx
