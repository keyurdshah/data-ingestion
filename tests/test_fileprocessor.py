import json
import unittest
import warnings
from unittest import TestCase

from pandas import DataFrame

from fileprocessor import FileProcessor


class TestFileProcessor(TestCase):


    def test_extract(self):
        """
        test large file loading with row size = 100
        :return: assert pass or fail
        """

        fp = FileProcessor(chunksize=100)
        with open('./testResources/transform.json') as file:
            fp.config(json.loads(file.read()))
        loc: str = './testResources/largefile.csv'
        for df in fp.extract(loc):
            self.assertTrue(df.count(axis=1).count() <= 100)

    def test_transform(self):
        """
         test large file loading with row size = 100
        :return:  assert pass or fail
        """
        fp = FileProcessor(chunksize=100)
        with open('./testResources/transform.json') as file:
            fp.config(json.loads(file.read()))

        loc: str = './testResources/largefile.csv'
        generator = fp.transform(loc)
        for df in generator:
            self.assertTrue(len(list(df.columns)), 6)

    #full load
    def test_load(self):
        fp = FileProcessor(chunksize=100)
        with open('./testResources/transform.json') as file:
            fp.config(json.loads(file.read()))
        loc = 'testResources/largefile.csv'

        for df in fp.load(loc):
            self.assertTrue(df is not None)


    def test_bad_data(self):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        fp = FileProcessor(chunksize=100)
        with open('./testResources/transform.json') as file:
            fp.config(json.loads(file.read()))
        loc = 'testResources/baddata.csv'
        with self.assertRaises(ValueError):
            with next(fp.load(loc)) as df:
                print(df)

