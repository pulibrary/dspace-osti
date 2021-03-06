import os
import json
import html

import requests
import pandas as pd

from os.path import join as pjoin

class Scraper:
    """Pipeline to collect data from OSTI & DSpace, comparing which datasets
     are not yet posted, and generating a form for a user to manually enter
     additional needed information."""
    def __init__(self, data_dir='data', osti_scrape='osti_scrape.json', 
            dspace_scrape='dspace_scrape.json', entry_form_full_path='entry_form.csv', 
            to_upload='dataset_metadata_to_upload.json'):

        data_dir = 'data'
        self.osti_scrape = pjoin(data_dir, osti_scrape)
        self.dspace_scrape = pjoin(data_dir, dspace_scrape)
        self.entry_form = entry_form_full_path
        self.to_upload = os.path.join(data_dir, to_upload)

        if not os.path.exists(data_dir):
            os.mkdir(data_dir)


    def get_existing_datasets(self):
        """Paginate through OSTI's Data Explorer API to find the datasets that have
        already been submitted.
        """
        MAX_PAGE_COUNT = 10
        existing_datasets = []

        for page in range(MAX_PAGE_COUNT):
            r = requests.get(f'https://www.osti.gov/dataexplorer/api/v1/records?site_ownership_code=PPPL&page={page}')
            j = json.loads(r.text)
            if len(j) != 0:
                existing_datasets.extend(j)
            else:
                print(f'Pulled {len(existing_datasets)} records from OSTI.')
                break
        else:
            raise BaseException("Didn't reach the final page of OSTI! Increase the variable MAX_PAGE_COUNT")

        with open(self.osti_scrape, 'w') as f:
            json.dump(existing_datasets, f, indent=4)

    
    def get_dspace_metadata(self):
        """Collect metadata on all items from all DSpace PPPL collections.
        
        collections = {
            'Plasma Science & Technology': 1422,
            'Theory and Computation': 2266,
            'Stellarators': 1308,    
            'NSTX': 1282,
            'NSTX-U': 1304
        }
        """
        COLLECTION_IDS = [1422, 2266, 1308, 1282, 1304]
        all_items = []

        for collection_id in COLLECTION_IDS:
            r = requests.get(
                f'https://dataspace.princeton.edu/rest/collections/{collection_id}/items?expand=metadata'
            )
            j = json.loads(r.text)
            all_items.extend(j)

        # Confirm that all collections were included
        PPPL_COMMUNITY_ID = 346
        r = requests.get(f"https://dataspace.princeton.edu/rest/communities/{PPPL_COMMUNITY_ID}")
        assert json.loads(r.text)['countItems'] == len(all_items), ("The number" + 
            " of items in the PPPL community does not equal the number of items" + 
            " collected. Review the list of collections we search through" +
            " (variable COLLECTION_IDS) and ensure that all PPPL collections" +
            " are included. Or write a recursive function to prevent this" +
            " from happening again.")

        print(f'Pulled {len(all_items)} records from DSpace.\n\n')
        with open(self.dspace_scrape, 'w') as f:
            json.dump(all_items, f, indent=4)


    def get_unposted_metadata(self):
        """Compare the OSTI and DSpace JSON to see what titles need to be uploaded.
        """

        with open(self.dspace_scrape) as f:
            dspace_j = json.load(f)
        with open(self.osti_scrape) as f:
            osti_j = json.load(f)

        osti_titles = [html.unescape(x['title']) for x in osti_j]
        dspace_titles = [x['name'] for x in dspace_j]

        titles_to_be_published = [x for x in dspace_titles if x not in osti_titles]
        to_be_published = [item for item in dspace_j if item['name'] in titles_to_be_published]

        with open(self.to_upload, 'w') as f:
            json.dump(to_be_published, f, indent=4)

        print(f"{len(to_be_published)} unpublished records were found.", end="\n\n")
        

        errors = [x for x in osti_titles if x not in dspace_titles]
        if len(errors) > 0:
            print(f"The following records were found on OSTI but not in DSpace (that" + 
                " shouldn't happen). If they closely resemble records we are about to" +
                " upload, please remove those records from from the upload process.")
            for error in errors:
                print("---  " + error)


    def generate_contract_entry_form(self):
        """Create a CSV where a user can enter Sponsoring Organizations, DOE
         Contract, and Datatype, additional information required by OSTI
        """
        with open(self.to_upload) as f:
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
        df.to_csv(self.entry_form, index=False)


    def run_pipeline(self, scrape=True):
        if scrape:
            self.get_existing_datasets()
            self.get_dspace_metadata()
        self.get_unposted_metadata()
        self.generate_contract_entry_form()


if __name__ == '__main__':
    s = Scraper()
    s.run_pipeline()