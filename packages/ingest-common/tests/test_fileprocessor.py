import json
import warnings
from unittest import TestCase
from ingestcommon.datatransform import DataTransform
from ingestcommon.fileprocessor import FileProcessor


class TestFileProcessor(TestCase):

    transfer_config = None
    def setUp(self):
        self.transform_config = DataTransform(cfg_path='../testResources/transform.json')

    def test_extract(self):
        """
        test large file loading with row size = 100
        :return: assert pass or fail
        """

        fp = FileProcessor(chunksize=100,transform=self.transform_config)
        loc: str = '../testResources/largefile.csv'
        for df in fp.extract(loc):
            self.assertTrue(df.count(axis=1).count() <= 100)

    def test_transform(self):
        """
         test large file loading with row size = 100
        :return:  assert pass or fail
        """
        fp = FileProcessor(chunksize=100,transform=self.transform_config)
        loc: str = '../testResources/largefile.csv'
        generator = fp.transform(loc)
        for df in generator:
            self.assertTrue(len(list(df.columns)), 6)

    #full load
    def test_load(self):
        fp = FileProcessor(chunksize=100,transform=self.transform_config)
        loc = '../testResources/largefile.csv'

        for df in fp.load(loc):
            self.assertTrue(df is not None)


    def test_bad_data(self):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        fp = FileProcessor(chunksize=100,transform=self.transform_config)
        loc = '../testResources/baddata.csv'
        with self.assertRaises(ValueError):
            with next(fp.load(loc)) as df:
                print(df)

