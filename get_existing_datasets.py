import requests
import json
from os.path import join as pjoin

data_dir = 'data'
existing_osti_datasets = pjoin(data_dir, 'existing_osti_datasets.json')

def get_existing_datasets():
    """Paginate through OSTI's Data Explorer API to find the datasets that have
    already been submitted.
    """
    existing_datasets = []

    MAX_PAGE_COUNT = 10

    for page in range(MAX_PAGE_COUNT):
        r = requests.get(f'https://www.osti.gov/dataexplorer/api/v1/records?site_ownership_code=PPPL&page={page}')
        j = json.loads(r.text)
        if len(j) != 0:
            print(page, end=', ')
            existing_datasets.extend(j)
        else:
            print(page)
            print(f'Pulled {len(existing_datasets)} records')
            print('OSTI scrape complete.')
            break
    else:
        raise BaseException("Didn't reach the final page! Increase the variable MAX_PAGE_COUNT")

    with open(existing_osti_datasets, 'w') as f:
        json.dump(existing_datasets, f, indent=4)

if __name__ == '__main__':
    get_existing_datasets()