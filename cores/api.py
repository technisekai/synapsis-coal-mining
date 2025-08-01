import requests

def read_from_api(url: str, headers: dict=None, params: dict=None):
    """
    Get data from api

    Arg(s):
        url: endpoint 
        headers: headers request
        
    Return(s):
        json response in dictionary data type
    """
    response = requests.get(url, headers=headers, params=params)

    return response.json()