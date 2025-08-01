import pandas as pd

def read_from_csv(path_file: str):
    """
    Read data from files

    Arg(s):
        path_file: path to file that wil be read. ex: path/to/file.csv

    Return(s):
        dictionary with format:
            [{
                'key': 'value', 
                'key': {'key': 'value'}
            }]
    """
    df = pd.read_csv(path_file)
    result = df.to_dict(orient='records')
    return result