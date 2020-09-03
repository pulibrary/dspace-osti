import requests
import json
from os.path import join as pjoin

data_dir = 'data'
dspace_dataset_metadata = pjoin(data_dir, 'dspace_dataset_metadata.json')

def get_dspace_metadata():
    """Collect metadata on all items from all DSpace PPPL collections.
    
    collections = {
        'Plasma Science & Technology': 1422,
        'Theory and Computation': 2266,
        'Stellarators': 1308,    
        'NSTX': 1282,
        'NSTX-U': 1304
    }
    """
    collection_ids = [1422, 2266, 1308, 1282, 1304]
    all_items = []

    for collection_id in collection_ids:
        r = requests.get(
            f'https://dataspace.princeton.edu/rest/collections/{collection_id}/items?expand=metadata', 
            verify=False
        )
        j = json.loads(r.text)
        all_items.extend(j)

    with open(dspace_dataset_metadata, 'w') as f:
        json.dump(all_items, f, indent=4)

if __name__ == '__main__':
    get_dspace_metadata()