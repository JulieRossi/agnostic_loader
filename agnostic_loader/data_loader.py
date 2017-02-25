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


def is_json_file(filename):
    with open(filename, "r") as f:
        try:
            json.loads(f.readline())
            return True
        except json.DecodeError:
            return False


def find_input_type(input_arg):
    if isinstance(input_arg, dict):
        return 'dict'
    if isinstance(input_arg, basestring):
        try:
            json.loads(input_arg)
            return "json"
        except json.DecodeError:
            if os.path.isfile(input_arg):
                if os.path.splitext(input_arg)[-1] == ".gz":
                    return "gzip"
                if is_json_file(input_arg):
                    return "json_file"
                return "csv_file"
            if os.path.exists(input_arg):
                return "dir"
    if hasattr(input_arg, "__iter__"):
        return "iter"
    return None


class DataLoader(object):
    __metaclass__ = abc.ABCMeta

    def __new__(cls, filename):
        input_type = find_input_type(filename)
        for sub in cls.__subclasses__():
            if sub.is_designed_for(input_type):
                o = object.__new__(sub)
                o.__init__(filename)
                return o

    def __init__(self, filename):
        self.filename = filename

    @classmethod
    def is_designed_for(cls, ext):
        if ext == cls.input_type:
            return True
        return False

    @abc.abstractmethod
    def load(self):
        return


class GzLoader(DataLoader):
    input_type = "gzip"

    def __init__(self, *args, **kwargs):
        self.loader = None
        super(GzLoader, self).__init__(*args, **kwargs)

    def load(self):
        with gzip.open(self.filename, "r") as f:
            for line in f:
                try:
                    self.loader.filename = line
                except AttributeError:
                    self.loader = DataLoader(line)
                    if self.loader is None:
                        self.loader = CsvFakeLoader(line)
                yield self.loader.load().next()


class JsonFileLoader(DataLoader):
    input_type = "json_file"

    def load(self):
        with open(self.filename, "r") as f:
            for line in f:
                yield json.loads(line)


class CsvFileLoader(DataLoader):
    input_type = "csv_file"

    def load(self):
        with open(self.filename, "r") as f:
            reader = csv.reader(f, delimiter=",")
            for line in reader:
                yield line


class JsonLoader(DataLoader):
    input_type = "json"

    def load(self):
        yield json.loads(self.filename)


class CsvFakeLoader(object):
    def __init__(self, filename):
        self.filename = filename

    def load(self):
        yield self.filename.strip("\n").split(",")


class DictLoader(DataLoader):
    input_type = 'dict'

    def load(self):
        yield self.filename


class DirLoader(DataLoader):
    input_type = "dir"

    def load(self):
        for fic in os.listdir(self.filename):
            fullname = os.path.join(self.filename, fic)
            loader = DataLoader(fullname)
            for elt in loader.load():
                yield elt


class GenericIterableLoader(DataLoader):
    input_type = "iter"

    def load(self):
        for elt in self.filename:
            yield elt
