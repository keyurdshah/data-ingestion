# Data Ingestion Service 
### Overview
    The data-ingestion is REST API service process the file write the transformed output as Stream. The file support largefiles size with default value of 10000 rows which is configurable as part of GET requirest query param


### Approach 
1. Provide csv-input file , tranformation config file path and output file path as command line argument 
```
python fileingestconsole.py --csv <csv-dir>/some.csv --transform <trasform-dir>/some.json --out <out-dir>/out.csv 
```
2. Read configuration file and build dictionary objects for `sourceColumns` , `mapping` , `targetColumns`
   * single JSON file structure to scale the   `FileProcessor` to support different CSV input with different type based column output.
3. We will use `pandas` python library to extract data, transform, and write to trasform output into various format.
   * #### Why `pandas` ?
     * `pandas` easty extract with method argument `thousands` sperator for decimal string 
     * `pandas` faster loading and chunking largefiles with method argument `chunkSize`
     * `pandas` abilty to transform types as column `Series` based approach rather iterating through row
     * `pandas` ability to write/load data 
5. Building `ingest-common\fileprocessor\FileProcessor` utility class which perform three basic operations 
   * 

### Implemetnation 

#### Artifacts

```
data-ingestion
├── ReadMe.md
├── packages
│   ├── ingest-common
│   │   ├── ingestcommon
│   │   │   ├── __init__.py
│   │   │   │   ├── __init__.cpython-311.pyc
│   │   │   │   ├── datatransform.cpython-311.pyc
│   │   │   │   └── fileprocessor.cpython-311.pyc
│   │   │   ├── datatransform.py
│   │   │   └── fileprocessor.py
│   │   ├── setup.py
│   │   └── tests
│   │       ├── __init__.py
│   │       │   ├── __init__.cpython-311.pyc
│   │       │   └── test_fileprocessor.cpython-311.pyc
│   │       └── test_fileprocessor.py
│   └── testResources
│       ├── baddata.csv
│       ├── largefile.csv
│       └── transform.json
├── requirements.txt
└── services
    ├── api
    │   └── ingestsapi
    │       └── __init__.py
    ├── cli
    │   ├── ingestcli
    │   │   ├── __init__.py
    │   │   └── fileingestconsole.py
    │   └── setup.py
    └── resources
        ├── largefile.csv
        └── transform.json
```

### Execution 

setup tools [in Porgress]
```
$ pip install -r requirements.txt
$ python fileingestconsole.py --csv <csv-dir>/some.csv --transform <trasform-dir>/some.json --out <out-dir>/out.csv 

Note: the current setup tool is in progress to run from CLI. for now use IntelliJ PyCharm/Ultimate

```
upon execution the file will generate output ate given path for argument `--out`.
if the file failed to process it will exit with error messaging. 

Exception Handling:
Catching all the exception and logging them to Logger("Default")