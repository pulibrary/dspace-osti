import json
import os
import datetime
import argparse
import sys

import pandas as pd

from ostiapi import ostiapi

class Poster:
    """Use the form input and DSpace metadata to generate the JSON necessary
     for OSTI ingestion. Then post to OSTI using their API"""
    def __init__(self, data_dir='data', to_upload='dataset_metadata_to_upload.json',
     form_input_full_path='form_input.csv', osti_upload='osti.json', response_output='osti_response.json'):
        self.form_input = form_input_full_path

        self.data_dir = data_dir
        self.to_upload = os.path.join(data_dir, to_upload)
        self.osti_upload = os.path.join(data_dir, osti_upload)
        self.response_output = os.path.join(data_dir, response_output)

        assert os.path.exists(data_dir)
        assert 'OSTI_USERNAME' in os.environ and 'OSTI_PASSWORD' in os.environ

    def generate_upload_json(self):
        """Validate the form input provided by the user and combine new data
         with DSpace data to generate JSON that is prepared for OSTI ingestion"""

        with open(self.to_upload) as f:
            to_upload_j = json.load(f)

        df = pd.read_csv(self.form_input)
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
            
            # get publication date
            date_info = [m['value'] for m in dspace_data['metadata'] if m['key'] == 'dc.date.available']
            assert len(date_info) == 1
            date_info = date_info[0]
            pub_dt = datetime.datetime.strptime(date_info, "%Y-%m-%dT%H:%M:%S%z")
            pub_date = pub_dt.strftime('%m/%d/%Y')

            # Collect all required information
            item_dict = {
                'title': dspace_data['name'],
                'creators': ';'.join([m['value'] for m in dspace_data['metadata'] if m['key'] == 'dc.contributor.author']),
                'dataset_type': row['Datatype'],
                'site_url': "https://dataspace.princeton.edu/handle" + dspace_data['handle'],
                'contract_nos': row['DOE Contract'],
                'sponsor_org': row['Sponsoring Organizations'],
                'research_org': 'PPPL',
                'accession_num': dspace_data['handle'],
                'publication_date': pub_date
            }

            # Collect optional required information
            abstract = [m['value'] for m in dspace_data['metadata'] if m['key'] == 'dc.description.abstract']
            if len(abstract) != 0:
                item_dict['description'] = '\n\n'.join(abstract)

            keywords = [m['value'] for m in dspace_data['metadata'] if m['key'] == 'dc.subject']
            if len(keywords) != 0:
                item_dict['keywords'] = ';'.join(keywords)

            osti_format.append(item_dict)

        with open(self.osti_upload, 'w') as f:
            json.dump(osti_format, f, indent=4)


    def _fake_post(self, record, username, password):
        """A fake JSON response that mirrors OSTI's"""
        return {'record': {'osti_id': '1488485',
          'accession_num': '88435/dsp01z316q451j',
          'product_nos': 'None',
          'title': 'Toward fusion plasma scenario planning for NSTX-U using machine-learning-accelerated models',
          'contract_nos': 'AC02-09CH11466',
          'other_identifying_nos': None,
          'doi': '10.11578/1488485',
          'doi_status': 'PENDING',
          'status': 'SUCCESS',
          'status_message': None,
          '@status': 'UPDATED'}}


    def post_to_osti(self, mode):
        """Post the collected metadata to OSTI's test or prod server. If in dev
         mode, call our _fake_post method"""
        if mode == 'test':
            ostiapi.testmode()

        response_data = []
        with open(self.osti_upload) as f:
            osti_j = json.load(f)

        for record in osti_j:
            if mode == 'dev':
                response = self._fake_post(record, os.environ['OSTI_USERNAME'], os.environ['OSTI_PASSWORD'])
            else:
                # response = ostiapi.post(record, os.environ['OSTI_USERNAME'], os.environ['OSTI_PASSWORD'])
                response = self._fake_post(record, os.environ['OSTI_USERNAME'], os.environ['OSTI_PASSWORD'])
            print(response)
            response_data.append(response)

        with open(self.response_output, 'w') as f:
            json.dump(response_data, f, indent=4)
        
        if mode != 'dev':
            if all([item['record']['status'] == 'SUCCESS' for item in response_data]):
                print("Congrats 🚀 OSTI says that all records were successfully uploaded!")
            else:
                print("Some of OSTI's responses do not have 'SUCCESS' as their" +
                    f" status. Look at the file {self.response_output} to" +
                    " see which records were not successfully uploaded.")


    def run_pipeline(self, mode):
        self.generate_upload_json()
        self.post_to_osti(mode)


if __name__ == '__main__':
    args = sys.argv

    help_s = """
Choose one of the following options:
    --dev: Make fake requests locally to test workflow.
    --test: Post to OSTI's test server.
    --prod: Post to OSTI's prod server.
    """
    
    commands = ['--dev', '--test', '--prod']

    if len(args) != 2 or args[1] in ['--help', '-h'] or args[1] not in commands:
        print(help_s)
    else:
        mode = args[1][2:]
        p = Poster()
        p.run_pipeline(mode)
