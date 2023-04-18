import requests


def get_data():
    endpoint = 'https://api.hibob.com/v1/people'
    headers = {'Authorization': ''}

    response = requests.get(endpoint, headers=headers).json()
    data = response['employees']

    return data