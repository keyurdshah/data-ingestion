import logging
import os.path
import sys
from os import getcwd

from fileprocessor import FileProcessor
import json

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




        fp = FileProcessor(chunksize=1)
        with open(file=configfile) as file:
            config:dict = json.loads(file.read())
            fp.config(config)
        with open(file=output,mode="w+") as outfile:
            for string in fp.load(csvfile):
                outfile.write(str(string))
    except FileNotFoundError as efx:
        logging.Logger("Default").error(msg=str(efx))
    except ValueError as ex:
        logging.Logger("Default").error(msg=str(efx))
    except Exception as ex:
       raise ex

