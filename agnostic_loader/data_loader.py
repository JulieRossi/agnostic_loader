"""
This module supplies Loader class that indifferently loads data from

- dict yields dict (no changes)
- iterator (list, generator) yield each element (no changes)
- json (string) -> loads json
- json file -> loads and yields each line
- csv file -> loads and yields each line
- gz file (recursively json or csv on each line of the file)
- directory (recursively loads each file)
"""

import abc
import csv
import gzip
import os

try:
    import cjson as json
    json.loads = json.decode
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import json
    json.DecodeError = ValueError


def is_json_by_line_file(input_arg):
    with open(input_arg, "r") as f:
        try:
            json.loads(f.readline())
            return True
        except json.DecodeError:
            return False


def find_input_loader(input_arg):
    if isinstance(input_arg, dict):
        return _DictLoader
    if isinstance(input_arg, basestring):
        try:
            json.loads(input_arg)
            return _JsonLoader
        except json.DecodeError:
            if os.path.isfile(input_arg):
                try:
                    with gzip.open(input_arg, "r") as f:
                        f.read(1)
                    return _GzLoader
                except IOError:
                    if is_json_by_line_file(input_arg):
                        return _JsonLineFileLoader
                    return _CsvFileLoader
            if os.path.exists(input_arg):
                return _DirLoader
    if hasattr(input_arg, "__iter__"):
        return _GenericIterableLoader
    return _CsvLoader


class DataLoader(object):
    __metaclass__ = abc.ABCMeta

    def __new__(cls, input_data):
        o = object.__new__(find_input_loader(input_data))
        o.__init__(input_data)
        return o

    def __init__(self, input_data):
        self.input_data = input_data

    @abc.abstractmethod
    def load(self):
        return


class _GzLoader(DataLoader):

    def __init__(self, *args, **kwargs):
        self.loader = None
        super(_GzLoader, self).__init__(*args, **kwargs)

    def load(self):
        with gzip.open(self.input_data, "r") as f:
            for line in f:
                try:
                    self.loader.input_data = line
                except AttributeError:
                    self.loader = DataLoader(line)
                    if self.loader is None:
                        self.loader = _CsvLoader(line)
                yield self.loader.load().next()  # there is only one line in loader.load generator


class _JsonLineFileLoader(DataLoader):

    def load(self):
        with open(self.input_data, "r") as f:
            for line in f:
                yield json.loads(line)


class _CsvFileLoader(DataLoader):

    def load(self):
        with open(self.input_data, "r") as f:
            reader = csv.reader(f, delimiter=",")
            for line in reader:
                yield line


class _JsonLoader(DataLoader):

    def load(self):
        yield json.loads(self.input_data)


class _CsvLoader(DataLoader):

    def load(self):
        yield self.input_data.strip("\n").split(",")


class _DictLoader(DataLoader):

    def load(self):
        yield self.input_data


class _DirLoader(DataLoader):

    def filter_on_filename(self, *args, **kwargs):
        return True

    def load(self):
        for fic in os.listdir(self.input_data):
            if self.filter_on_filename(fic):
                fullname = os.path.join(self.input_data, fic)
                loader = DataLoader(fullname)
                for elt in loader.load():
                    yield elt


class _GenericIterableLoader(DataLoader):

    def load(self):
        for elt in self.input_data:
            yield elt
