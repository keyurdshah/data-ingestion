import json


class DataTransform:
    config_path: str = None

    def __init__(self, cfg_path: str):
        self.config_path = cfg_path

    @property
    def Config(self) -> dict:
        '''
        Get configuration for transformation for a given config file path
        {
            'source_column': {
                'name': 'new column name' -- can be null
                'type': 'Ingteger/String/Object' int,str,float,object(for date)
                'combine':['src_col1','src_col2']
                'suffix': 'suffix to value'
                'regex' : '' for validation
            }
        }
        :return: JSON dictionary
        '''
        with open(file=self.config_path) as file:
            config: dict = json.loads(file.read())
        return config
