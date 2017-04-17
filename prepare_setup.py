import os

version = os.getenv("TRAVIS_TAG")

assert version

with open(os.path.join(os.path.dirname(__file__), "setup.tmpl"), "r") as tmpl_f, \
        open(os.path.join(os.path.dirname(__file__), "setup.py"), "w") as py_f:
    py_f.write(tmpl_f.read().replace("{{VERSION}}", version))
