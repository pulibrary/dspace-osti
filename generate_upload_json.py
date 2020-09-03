import json
import os
import pandas as pd

data_dir = "data"
to_upload = os.path.join(data_dir, "dataset_metadata_to_upload.json")
input_form = os.path.join(data_dir, "form_input.csv")
osti_upload = os.path.join(data_dir, "osti.json")

def generate_upload_json():
    with open(to_upload) as f:
        to_upload_j = json.load(f)

    df = pd.read_csv(input_form)
    df = df.set_index('DSpace ID')

    # Validate Input CSV 
    def no_empty_cells(series):
        return series.shape[0] == series.dropna().shape[0]
    def unique_values(series):
        return series.shape[0] == series.unique().shape[0]

    expected_columns = ['Sponsoring Organizations', 'DOE Contract', 'Datatype']
    assert all([col in df.columns for col in expected_columns])
    assert no_empty_cells(df['Sponsoring Organizations'])
    assert no_empty_cells(df['DOE Contract'])
    assert no_empty_cells(df['Datatype'])
    assert no_empty_cells(df['Author'])

    accepted_datatype_values = ['AS','GD', 'IM', 'ND', 'IP', 'FP', 'SM', 'MM', 'I']
    assert all([dt in accepted_datatype_values for dt in df['Datatype']])


    # Generate final JSON to post to OSTI
    osti_format = []
    for dspace_id, row in df.iterrows():
        dspace_data = [item for item in to_upload_j if item['id'] == dspace_id]
        assert len(dspace_data) == 1
        dspace_data = dspace_data[0]
        
        osti_format.append({
            'title': dspace_data['name'],
            'creators': ';'.join([m['value'] for m in dspace_data['metadata'] if m['key'] == 'dc.contributor.author']),
            'dataset_type': row['Datatype'],
            'site_url': "https://dataspace.princeton.edu/handle" + dspace_data['handle'],
            'contract_nos': row['DOE Contract'],
            'sponsor_org': row['Sponsoring Organizations']
        })

    with open(osti_upload, 'w') as f:
        json.dump(osti_format, f, indent=4)

if __name__ == '__main__':
    generate_upload_json()