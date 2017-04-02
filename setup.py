import os

from setuptools import setup

version = os.getenv("TRAVIS_TAG")

if version:
    setup(name='agnostic_loader',
          version=version,
          description='Load data from different input sources',
          url='https://github.com/JulieRossi/agnostic_loader.git',
          author='Julie Rossi',
          author_email='julierossi06@gmail.com',
          license='MIT',
          packages=['agnostic_loader'],
          zip_safe=False)
