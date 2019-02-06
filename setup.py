from setuptools import Extension, setup


setup(name='splitwork',
      version='0.1',
      description='A command line utility and a Python library for parallel execution',
      author='Rafael Carrascosa',
      packages=['splitwork'],
      ext_modules=[Extension("_split_merge", sources=['splitwork/_split_merge.c'])],
      scripts=["scripts/splitwork"],
      )
