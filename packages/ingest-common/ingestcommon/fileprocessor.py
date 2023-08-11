"Read CSV files process and transfrom in to target dataframe"
import io
import logging
import traceback

import pandas
from pandas import DataFrame

from ingestcommon.datatransform import DataTransform

logger = logging.getLogger()


class FileProcessor:
    chunks: int = None
    trfm_config: dict = None


    def __init__(self, transform: DataTransform, chunksize: int = 1000):
        self.chunks = chunksize
        self.trfm_config = transform.Config

    def extract(self, loc: str) -> DataFrame:
        """
        Read the CSV files in chunks
        :param loc: file path/URL path
        :return: row
        """
        # using pandas library for reading file in chunks rather than full file in memory

        reader = pandas.read_csv(loc, chunksize=self.chunks, thousands=',')
        for df in reader:
            yield df

    def _get_dtype(self, type: str) -> type:
        """
        Translate String value to Platform suppported data type type
        :param type: String input
        :return: Type instance of perticular String input stype type e.g. "Integer" represents `int` in python
        """
        if (type == "Integer"):
            return int
        if (type == "String"):
            return str
        if (type == "Object"):
            return object
        if (type == "BigDecimal"):
            return float
        if (type == "Boolean"):
            return bool
        raise Exception("Type not supported")

    def transform(self, loc: str):
        """
        Apply transformation for given file
        :param loc: file path/ URL path
        :return: dataframe reference for a given chunk read of file
        """
        for df in self.extract(loc):
            yield self._get_transform(df)

    def _get_transform(self, df: DataFrame):
        """
        apply Transformation input filestream
        :param loc: filepath/URL apth
        :param transformConfig: dictionary of source columns dereived from self.config() execution
        :return: yielding dataframe for given chunkSize
        """
        rows = 0

        try:
            # read only necessary source columns
            src_df = DataFrame(df, columns=list(self.trfm_config.keys()))

            # perform transformation
            out_df = DataFrame(data={})

            for k in self.trfm_config.keys():
                v = self.trfm_config[k]
                # name of the target column
                name = v.get('name')
                # String type to Platform Types
                type = v.get('type')
                if name:
                    out_df[name] = src_df[k].astype(self._get_dtype(type))
                else:  # no rename (either a new column with suffix or combining source column)
                    if v.get('combine') and v.get('type') == 'Object':
                        out_df[k] = pandas.to_datetime(src_df[v.get('combine')], format=v.get('format'))
                    if v.get('suffix'):
                        out_df[k] = src_df[k].fillna('') + 'kg'

            rows += src_df.shape[0]
            return out_df
        except Exception as ex:
            logger.error("Data Error after row: " + str(rows) + " Error: " + str(ex))
            raise ex

    def load(self, loc: str) -> str:
        """
        Generate the output file for given input file @ loc
        :param loc: input file path or URL path (pandas support reading directly from web )
        :return: yielding stringBuffer so caller can write batches of chunks
        """
        header = True
        for target_df in self.transform(loc):
            if (header):
                output: io.StringIO = io.StringIO()
                target_df.to_csv(output, header=header, index=False)
                header = False
                yield output.getvalue()
                output.close()
            else:
                output: io.StringIO = io.StringIO()
                target_df.to_csv(output, header=header, index=False)
                yield output.getvalue()
                output.close()

# if __name__ == "__main__":
#
#     fp = FileProcessor(chunksize=1)
#     loc = 'https://gist.githubusercontent.com/daggerrz/99e766b4660e3c0ed26517beaea6449a/raw/e2d3a3e42ad1895baa430612f921bc87cfff651c/orders.csv'
#     header = True
#
#     for chunk in fp.load(loc, ingest.source, ingest.mapping, ingest.target, ingest.trsfm_conf):
#         print(str(chunk))
