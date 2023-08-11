import json
import logging
import sys

from ingestcommon.datatransform import DataTransform
from ingestcommon.fileprocessor import FileProcessor

if __name__ == "__main__":
    try:
        args = sys.argv[1:]
        print(args)

        for i in range(len(args)):
            if args[i].startswith('-'):
                if args[i] == '--csv':
                    csvfile = args[i+1]
                    ++i
                    continue
                if args[i] == '--transform':
                    configfile = args[i+1]
                    ++i
                    continue
                if args[i] == '--out':
                    output = args[i+1]
                    ++i
                    continue

        cfg:DataTransform = DataTransform(configfile)
        fp = FileProcessor(transform=cfg, chunksize=1)

        with open(file=output,mode="w+") as outfile:
            for string in fp.load(csvfile):
                outfile.write(str(string))
    except FileNotFoundError as ex:
        logging.Logger("Default").error(msg=str(ex))
    except ValueError as ex:
        logging.Logger("Default").error(msg=str(ex))
    except Exception as ex:
       raise ex

