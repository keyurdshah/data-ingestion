"Read CSV files process and transfrom in to target dataframe"
import io
import logging
import traceback

import pandas
from pandas import DataFrame

logger = logging.getLogger()
class FileProcessor:
    chunks:int = None
    sourceColumns: dict = None
    targetColumns: dict = None
    mappingColumns: dict = None


    def __init__(self, chunksize: int = 1000):
        self.chunks = chunksize

    def extract(self, loc: str) -> DataFrame:
        """
        Read the CSV files in chunks
        :param loc: file path/URL path
        :return: yielding row dataframe of given CSV without transformation
        """
        # using pandas library for reading file in chunks rather than full file in memory

        reader = pandas.read_csv(loc, chunksize=self.chunks, thousands=',')
        for df in reader:
            yield df

    def _get_dtype(self, type:str) -> type:
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
    def config(self, trsfm_conf:dict):
        self.mappingColumns ={}
        self.targetColumns={}
        self.sourceColumns = trsfm_conf

        for k in trsfm_conf.keys():
            v = trsfm_conf[k]
            #name of the target column
            name = v.get('name')
            #String type to Platform Types
            type = v.get('type')
            if name:
                self.mappingColumns[k] = name
                self.targetColumns[name] = self._get_dtype(type)
            else:
                self.targetColumns[k] = self._get_dtype(type)


    def transform(self, loc:str):
        """
        Apply transformation for given file
        :param loc: file path/ URL path
        :return: dataframe reference for a given chunk read of file
        """
        return self._transform(loc,self.sourceColumns,self.mappingColumns,self.targetColumns)
    def _transform(self, loc: str, source: dict, mapping: dict, target: dict):
        """
        apply Transformation input filestream
        :param loc: filepath/URL apth
        :param source: dictionary of source columns dereived from self.config() execution
        :param mapping: dictionary of mapping source:target columns
        :param target: dictionary of target output columns derived from self.config() execution
        :return: yielding dataframe for given chunkSize
        """
        rows = 0
        for df in self.extract(loc):
            try:
                # read only necessary source columns
                src_df = DataFrame(df, columns=list(source.keys()))

                # perform transformation

                # rename the columns using 1-1 rename mapping
                src_df = src_df.rename(columns=mapping)

                # apply the final output data types
                out_df = DataFrame(src_df, columns=list(target.keys())).astype(target)

                # add column special case of string to date
                out_df['OrderDate'] = pandas.to_datetime(
                    src_df['Year'].map(str) + '-' + src_df['Month'].map(str) + '-' + src_df['Day'].map(str),
                    format="%Y-%m-%d")
                out_df['Unit'] = 'kg'
                rows+= src_df.shape[0]
                yield out_df
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
