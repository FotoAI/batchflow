from batchflow.core.node import ConsumerNode
import csv


class FileAppenderConsumer(ConsumerNode):
    def __init__(
        self,
        output,
        key=None,
        header=True,
        index=True,
        group_by=False,
        group_by_column_name=None,
        mode="w",
    ):
        self.output_file = output
        self._header = header
        self._first_item = True
        self._file = None
        self._writer = None
        self.key = key
        self.index = index
        self.group_by = group_by
        if self.index:
            self._index_counter = 1
        if self.group_by:
            self._group_by_counter = 1
        self._mode = mode

        self.group_by_column_name = group_by_column_name
        super(FileAppenderConsumer, self).__init__()

    def open(self):
        self._file = open(self.output_file, self._mode)
        self._writer = csv.writer(self._file)

    def close(self):
        self._file.close()

    def consume(self, item):
        # item should be serializable, otherwise error.
        if self.key is not None:
            item = item[self.key]
        try:
            if not isinstance(item, list):
                item = [item]

            if self._header and self._first_item:
                row = []
                try:
                    if self.index:
                        row.append("index")
                    if self.group_by:
                        row.append(self.group_by_column_name)

                    row.extend(list(item[0].keys()))
                    self._writer.writerow(row)
                    self._first_item = False
                except IndexError:
                    print("data is None will try to take header in next iteration")

            for i in item:
                row = []
                if self.index:
                    row.append(self._index_counter)
                    self._index_counter += 1
                if self.group_by:
                    row.append(self._group_by_counter)
                row.extend(list(i.values()))
                self._writer.writerow(row)

            if self.group_by:
                self._group_by_counter += 1
        except TypeError:
            print("Type Error")
