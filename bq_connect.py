from google.cloud import bigquery
from pathlib import Path
from google.oauth2 import service_account


def get_credentials():
    try:
        key_path = Path.cwd() / 'key.json'
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return credentials

    except Exception as error:
        print(f'Failed to read key.json: {error}')
        return None

def init_bigquery_client():
    project_id = 'belkins-3c679'
    credentials_object = get_credentials()
    return bigquery.Client(project='belkins-3c679', credentials=credentials_object)