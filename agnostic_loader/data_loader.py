"""
This module supplies DataLoader class that indifferently loads data from

- dict -> yield dict (no changes)
- iterator (list, generator) -> yield each element (no changes)
- json (string) -> yield json loaded (json.loads)
- json file (one json per line in file) -> loads and yields each line
- csv (string) -> yield csv read (csv.reader)
- csv file -> loads and yield each line
- gz file (recursively json or csv on each line of the file)
- directory (recursively loads each file)

Note that csv is expected to have comma as delimiter.
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
    """
    Check is input_arg file contains lines of json.

    Actually, checks if first line of input_arg is a json.

    :param input_arg: full path filename (should be openable with open function)
    :return: boolean
    """
    with open(input_arg, "r") as f:
        try:
            json.loads(f.readline())
            return True
        except json.DecodeError:
            return False


def find_input_loader(input_arg):
    """
    Test input_arg to determine with private loader (defined in this module) should be used.

    :param input_arg: can be
                - dictionary
                - other iterable
                - json (string)
                - json file (one json per line in file)
                - csv (string)
                - csv file
                - gz file
                - directory
    :return: selected loader class
    """
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
    """
    Factory that will be instantiated with the right loader (from private subclasses).

    Public attributes:
    input_data: raw input that needs to be loaded

    Public methods:
    load: yield each data loaded from input_data
    """
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
    """
    Load gzipped file.

    Public attributes:
    input_data: gzipped full path filename

    Public methods:
    load: Open gzipped file (self.input_data), find the right loader for data inside (json or csv)
            and yield each line loaded.
    """

    def __init__(self, *args, **kwargs):
        self.loader = None
        super(_GzLoader, self).__init__(*args, **kwargs)

    def load(self):
        """
        Load data in self.input_data

        :return: yield each line in self.input_data correctly loaded (with json or csv)
        """
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
    """
    Load file containing one json per line.

    Public attributes:
    input_data: full path filename of file to load (containing one json per line)

    Public methods:
    load: Open the file in self.input_data and yield each line loaded with json.loads()
    """

    def load(self):
        """
        Load data in self.input_data

        :return: yield each line in self.input_data json loaded
        """
        with open(self.input_data, "r") as f:
            for line in f:
                yield json.loads(line)


class _CsvFileLoader(DataLoader):
    """
    Load csv file (with ',' as delimiter).

    Public attributes:
    input_data: full path filename of csv file to load

    Public methods:
    load: Open the file in self.input_data and yield each line loaded with csv.reader()
    """

    def load(self):
        """
        Load data in self.input_data

        :return: yield each line in self.input_data csv loaded (with ',' as delimiter)
        """
        with open(self.input_data, "r") as f:
            reader = csv.reader(f, delimiter=",")
            for line in reader:
                yield line


class _JsonLoader(DataLoader):
    """
    Load json string.

    Public attributes:
    input_data: json string

    Public methods:
    load: load data in self.input_data using json.loads() and yield result
    """

    def load(self):
        """
        Load data in self.input_data

        :return: yield self.input_data json loaded
        """
        yield json.loads(self.input_data)


class _CsvLoader(DataLoader):
    """
    Load csv string.

    Public attributes:
    input_data: csv string

    Public methods:
    load: split in self.input_data by ',' and yield result
    """

    def load(self):
        """
        Load data in self.input_data

        :return: yield self.input_data splitted on ','
        """
        yield self.input_data.strip("\n").split(",")


class _DictLoader(DataLoader):
    """
    Idempotent class - artifice to use the same interface (DataLoader) even on loaded data.

    Public attributes:
    input_data: dictionary

    Public methods:
    load: yield self.input_data
    """

    def load(self):
        """
        yield self.input_data
        """
        yield self.input_data


class _DirLoader(DataLoader):
    """
    Load data from directory.

    Public attributes:
    input_data: directory full path

    Public methods:
    load: iter on self.input_data files and load each file using the right loader
            then yield each line in each file with the right loader.
    """

    def filter_on_filename(self, *args, **kwargs):
        """
        Can be overloaded to filter which file to load based on filename.

        :return: boolean
        """
        return True

    def load(self):
        """
        Load data in self.input_data

        :return: yield each line in each file in self.input_data,
                    using the right load method (gz, json or csv)
        """
        for fic in sorted(os.listdir(self.input_data)):
            if self.filter_on_filename(fic):
                fullname = os.path.join(self.input_data, fic)
                loader = DataLoader(fullname)
                for elt in loader.load():
                    yield elt


class _GenericIterableLoader(DataLoader):
    """
    Load data from iterator.

    Public attributes:
    input_data: iterable

    Public methods:
    load: yield each element in self.input_data
    """

    def load(self):
        """
        yield each element in self.input_data
        """
        for elt in self.input_data:
            yield elt
