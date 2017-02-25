import pytest

from data_loader import *

PATH = os.path.join(os.path.dirname(__file__), "resources")


@pytest.yield_fixture(scope="module")
def setup():
    to_gzip = [os.path.join(PATH, "input", "gz_file_json"),
               os.path.join(PATH, "input", "gz_file_csv")]
    for filename in to_gzip:
        with open(filename, "r") as in_fd, gzip.open("{}.gz".format(filename), "w") as out_fd:
            out_fd.write(in_fd.read())
    yield
    for filename in to_gzip:
        os.remove("{}.gz".format(filename))


def test01_dict():
    ipt = {"a": 1, "b": 2}
    l = DataLoader(ipt)
    assert l.__class__ == DictLoader
    assert list(l.load()) == [ipt]


def test02_list():
    ipt = [{"a": 1}, {"b": 2}]
    l = DataLoader(ipt)
    assert l.__class__ == GenericIterableLoader
    assert list(l.load()) == ipt


def test03_gen():
    ipt = (elt for elt in [{"a": 1}, {"b": 2}])
    l = DataLoader(ipt)
    assert l.__class__ == GenericIterableLoader
    assert list(l.load()) == [{"a": 1}, {"b": 2}]


def test04_gen():
    ipt = {"a": 1, "b": 2}.iteritems()
    l = DataLoader(ipt)
    assert l.__class__ == GenericIterableLoader
    assert list(l.load()) == [("a", 1), ("b", 2)]


def test04_json():
    ipt = '{"a": 1, "b": 2}'
    l = DataLoader(ipt)
    assert l.__class__ == JsonLoader
    assert list(l.load()) == [{"a": 1, "b": 2}]


def test05_json_file():
    ipt = os.path.join(PATH, "input", "json_file")
    l = DataLoader(ipt)
    assert l.__class__ == JsonFileLoader
    assert list(l.load()) == [{"a": 1, "b": 2}, {"a": 1, "b": 4}]


def test06_csv_file():
    ipt = os.path.join(PATH, "input", "csv_file")
    l = DataLoader(ipt)
    assert l.__class__ == CsvFileLoader
    assert list(l.load()) == [["a", '1'], ["b", '2']]


def test07_dir_json():
    ipt = os.path.join(PATH, "input", "dir_json")
    l = DataLoader(ipt)
    assert l.__class__ == DirLoader
    res = list(l.load())
    expected = [{"a": 1, "b": 2}, {"a": 1, "b": 4}, {"a": 1, "b": 4}]
    for elt in res:
        assert elt in expected
    for elt in expected:
        assert elt in res


def test08_dir_csv():
    ipt = os.path.join(PATH, "input", "dir_csv")
    l = DataLoader(ipt)
    assert l.__class__ == DirLoader
    res = list(l.load())
    expected = [["a", '1'], ["b", '2'], ["a", '1'], ["b", '4']]
    for elt in res:
        assert elt in expected
    for elt in expected:
        assert elt in res


def test09_gz_json(setup):
    ipt = os.path.join(PATH, "input", "gz_file_json.gz")
    l = DataLoader(ipt)
    assert l.__class__ == GzLoader
    assert list(l.load()) == [{"a": 1, "b": 2}, {"a": 1, "b": 4}]


def test10_gz_csv(setup):
    ipt = os.path.join(PATH, "input", "gz_file_csv.gz")
    l = DataLoader(ipt)
    assert l.__class__ == GzLoader
    assert list(l.load()) == [["a", '1'], ["b", '2']]
