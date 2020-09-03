import json
import os
import pandas as pd

data_dir = "data"
entry_form = os.path.join(data_dir, "entry_form.csv")

def generate_contract_entry_form():
    to_upload = os.path.join(data_dir, "dataset_metadata_to_upload.json")
    with open(to_upload) as f:
        to_upload_j = json.load(f)


    df = pd.DataFrame()
    df['DSpace ID'] = [item['id'] for item in to_upload_j]
    df['Issue Date'] = [[m['value'] for m in item['metadata'] if m['key'] == 'dc.date.issued'][0] for item in to_upload_j]
    df['Title'] = [item['name'] for item in to_upload_j]
    df['Author'] = [';'.join([m['value'] for m in item['metadata'] if m['key'] == 'dc.contributor.author']) for item in to_upload_j]
    df['Dataspace Link'] = ["https://dataspace.princeton.edu/handle/" + item['handle'] for item in to_upload_j]

    # To be filled in:
    df['Sponsoring Organizations'] = None
    df['DOE Contract'] = None
    df['Datatype'] = None

    df = df.sort_values('Issue Date')
    df.to_csv(entry_form, index=False)

if __name__ == '__main__':
    generate_contract_entry_form()
