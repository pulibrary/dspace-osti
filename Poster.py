import datetime
import json
import os
import sys

import pandas as pd

import ostiapi


class Poster:
    """Use the form input and DSpace metadata to generate the JSON necessary
     for OSTI ingestion. Then post to OSTI using their API"""
    def __init__(
        self, mode, data_dir='data', to_upload='dataset_metadata_to_upload.json',
        form_input_full_path='form_input.tsv', osti_upload='osti.json', response_dir="responses"
    ):
        self.mode = mode

        # Prepare all paths
        self.form_input = form_input_full_path
        self.data_dir = data_dir
        self.to_upload = os.path.join(data_dir, to_upload)
        self.osti_upload = os.path.join(data_dir, osti_upload)

        self.response_output = os.path.join(
            response_dir,
            f"{mode}_osti_response_{str(datetime.datetime.now()).replace(':', '')}.json"
        )

        assert os.path.exists(data_dir)
        assert os.path.exists(response_dir)

        # Ensure minimum (test/prod) environment variables are prepared
        if mode in ['test', 'prod']:
            environment_vars = [v + f'_{mode.upper()}' for
                                v in ['OSTI_USERNAME', 'OSTI_PASSWORD']]
        if mode == 'dry-run':
            environment_vars = ['OSTI_USERNAME_TEST', 'OSTI_PASSWORD_TEST',
                                'OSTI_USERNAME_PROD', 'OSTI_PASSWORD_PROD']

        assert all([var in os.environ for var in environment_vars]), \
            f'All {mode} environment variables need to be set. ' \
            f'See the README for more information.'

        # Assign username and password depending on where data is being posted
        if mode in ['test', 'prod']:
            self.username = os.environ[f'OSTI_USERNAME_{mode.upper()}']
            self.password = os.environ[f'OSTI_PASSWORD_{mode.upper()}']
        else:
            self.username, self.password = None, None

    def generate_upload_json(self):
        """Validate the form input provided by the user and combine new data
         with DSpace data to generate JSON that is prepared for OSTI ingestion"""

        with open(self.to_upload) as f:
            to_upload_j = json.load(f)

        df = pd.read_csv(self.form_input, sep='\t', keep_default_na=False)
        df = df.set_index('DSpace ID')

        # Validate Input CSV 
        def no_empty_cells(series):
            return series.shape[0] == series.dropna().shape[0]

        expected_columns = ['Sponsoring Organizations', 'DOE Contract', 'Datatype']
        assert all([col in df.columns for col in expected_columns]), \
            f"You're missing one of these columns {expected_columns}"
        assert no_empty_cells(df['Sponsoring Organizations']), \
            f"You have empty values in the Sponsoring Organizations column. This is a required value."
        assert no_empty_cells(df['DOE Contract']), \
            f"You have empty values in the DOE Contract column. This is a required value."
        assert no_empty_cells(df['Datatype']), \
            f"You have empty values in the Datatype column. This is a required value."

        accepted_datatype_values = ['AS', 'GD', 'IM', 'ND', 'IP', 'FP', 'SM', 'MM', 'I']
        assert all([dt in accepted_datatype_values for dt in df['Datatype']]), \
            "The Datatype column contains improper datatype values. " \
            f"The accepted datatype values are: {accepted_datatype_values}"

        # Generate final JSON to post to OSTI
        osti_format = []
        for dspace_id, row in df.iterrows():
            dspace_data = [item for item in to_upload_j if item['id'] == dspace_id]
            assert len(dspace_data) == 1, dspace_data
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
                'creators': ';'.join(
                    [
                        m['value']
                        for m in dspace_data['metadata']
                        if m['key'] == 'dc.contributor.author'
                    ]
                ),
                'dataset_type': row['Datatype'],
                'site_url': "https://dataspace.princeton.edu/handle/" + dspace_data['handle'],
                'contract_nos': row['DOE Contract'],
                'sponsor_org': row['Sponsoring Organizations'],
                'research_org': 'PPPL',
                'accession_num': dspace_data['handle'],
                'publication_date': pub_date,
                'othnondoe_contract_nos': row['Non-DOE Contract'],
            }

            # Collect optional required information
            abstract = [m['value'] for m in dspace_data['metadata'] if m['key'] == 'dc.description.abstract']
            if len(abstract) != 0:
                item_dict['description'] = '\n\n'.join(abstract)

            keywords = [m['value'] for m in dspace_data['metadata'] if m['key'] == 'dc.subject']
            if len(keywords) != 0:
                item_dict['keywords'] = '; '.join(keywords)

            is_referenced_by = [m['value'] for m in dspace_data['metadata'] if
                                m['key'] == 'dc.relation.isreferencedby']
            if len(is_referenced_by) != 0:
                item_dict['related_identifiers'] = []
                for irb in is_referenced_by:
                    item_dict['related_identifiers'].append({
                        'related_identifier': irb.split('doi.org/')[1],
                        'relation_type': 'IsReferencedBy',
                        'related_identifier_type': 'DOI',
                    })

            osti_format.append(item_dict)

        with open(self.osti_upload, 'w') as f:
            json.dump(osti_format, f, indent=4)

    @staticmethod
    def _fake_post(records):
        """A fake JSON response that mirrors OSTI's"""
        return {
            "record": [
                {
                    "osti_id": "1488485",
                    "accession_num": record["accession_num"],
                    "product_nos": "None",
                    "title": record["title"],
                    "contract_nos": record["contract_nos"],
                    "other_identifying_nos": None,
                    "othnondoe_contract_nos": record["othnondoe_contract_nos"],
                    "doi": "10.11578/1488485",
                    "doi_status": "PENDING",
                    "status": "SUCCESS",
                    "status_message": None,
                    "@status": "UPDATED"
                } for record in records
            ]
        }

    def post_to_osti(self):
        """Post the collected metadata to OSTI's test or prod server. If in
         dry-run mode, call our _fake_post method"""
        if self.mode == 'test':
            ostiapi.testmode()

        with open(self.osti_upload) as f:
            osti_j = json.load(f)

        print('Posting data...')
        if self.mode == 'dry-run':
            response_data = self._fake_post(osti_j)
        else:
            response_data = ostiapi.post(osti_j, self.username, self.password)

        with open(self.response_output, 'w') as f:
            json.dump(response_data, f, indent=4)
        
        # output results to the shell:
        for item in response_data['record']:
            if item['status'] == 'SUCCESS':
                print(f"\t✔ {item['title']}")
            else:
                print(f"\t✗ {item['title']}")

        if self.mode != 'dry-run':
            if all([item['status'] == 'SUCCESS' for item in response_data['record']]):
                print("Congrats 🚀 OSTI says that all records were successfully uploaded!")
            else:
                print(
                    "Some of OSTI's responses do not have 'SUCCESS' as their" +
                    f" status. Look at the file {self.response_output} to" +
                    " see which records were not successfully uploaded."
                )

    def run_pipeline(self):
        self.generate_upload_json()
        self.post_to_osti()


if __name__ == '__main__':
    args = sys.argv

    help_s = """
Choose one of the following options:
    --dry-run: Make fake requests locally to test workflow.
    --test: Post to OSTI's test server.
    --prod: Post to OSTI's prod server.
    """
    
    commands = ['--dry-run', '--test', '--prod']

    if len(args) != 2 or args[1] in ['--help', '-h'] or args[1] not in commands:
        print(help_s)
    else:
        mode = args[1][2:]
        p = Poster(mode)
        if mode == 'dry-run':
            user_response = 'yes'
        if mode in ['test', 'prod']:
            print(f"WARNING: Running this script in {mode} mode will "
                  "trigger emails to PPPL and OSTI!")
            user_response = input(
                "Are you sure you wish you proceed? (Enter 'Yes'/'yes') "
            )
        print(f"User response: {user_response}")
        if user_response.lower() == 'yes':
            p.run_pipeline()
        else:
            print("Exiting!!! You must respond with a Yes/yes")
