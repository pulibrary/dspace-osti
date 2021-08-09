import os
import json

import requests
import pandas as pd

from os.path import join as pjoin


class Scraper:
    """Pipeline to collect data from OSTI & DSpace, comparing which datasets
     are not yet posted, and generating a form for a user to manually enter
     additional needed information."""
    def __init__(self, data_dir='data', osti_scrape='osti_scrape.json',
                 dspace_scrape='dspace_scrape.json',
                 entry_form_full_path='entry_form.tsv',
                 to_upload='dataset_metadata_to_upload.json',
                 redirects='redirects.json'):

        self.osti_scrape = pjoin(data_dir, osti_scrape)
        self.dspace_scrape = pjoin(data_dir, dspace_scrape)
        self.entry_form = entry_form_full_path
        self.to_upload = pjoin(data_dir, to_upload)
        self.redirects = pjoin(data_dir, redirects)

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
            'NSTX': 1282,
            'NSTX-U': 1304,
            'Stellarators': 1308,
            'Plasma Science & Technology': 1422,
            'Theory and Computation': 2266,
            'ITER and Tokamaks PPPL Collaborations': 3378,
            'Theory': 3379,
            'Computational Science PPPL Collaborations': 3380,
            'Engineering Research': 3381,
            'ESH Technical Reports': 3382,
            'IT PPPL Collaborations': 3383
        }
        """
        # NOTE: The Dataspace REST API can now support requests from handles.
        #  Shifting this scrape to collection handles instead of IDs may make
        #  this script clearer and easier to change if necessary.
        COLLECTION_IDS = [1282, 1304, 1308, 1422, 2266, 3378, 3379, 3380, 3381, 3382, 3383]

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
        assert json.loads(r.text)['countItems'] == len(all_items),\
            ("The number of items in the PPPL community does not equal the "
             "number of items collected. Review the list of collections we "
             "search through (variable COLLECTION_IDS) and ensure that all "
             "PPPL collections are included. Or write a recursive function "
             "to prevent this from happening again.")

        print(f'Pulled {len(all_items)} records from DSpace.')
        with open(self.dspace_scrape, 'w') as f:
            json.dump(all_items, f, indent=4)

    def get_unposted_metadata(self):
        """Compare the OSTI and DSpace JSON to see what titles need to be uploaded.
        """
        def get_handle(doi, redirects_j):
            if doi not in redirects_j:
                r = requests.get(doi)
                assert r.status_code == 200, f"Error parsing DOI: {doi}"
                handle = r.url.split('handle/')[-1]
                redirects_j[doi] = handle
                return handle
            else:
                return redirects_j[doi]

        with open(self.redirects) as f:
            redirects_j = json.load(f)
        with open(self.dspace_scrape) as f:
            dspace_j = json.load(f)
        with open(self.osti_scrape) as f:
            osti_j = json.load(f)

        # Find handles in DSpace whose handles aren't linked in OSTI's DOIs
        # HACK: returning proper DOI while also updating redirects_j
        osti_handles = [get_handle(record['doi'], redirects_j)
                        for record in osti_j]

        to_be_published = []
        for dspace_record in dspace_j:
            if dspace_record['handle'] not in osti_handles:
                to_be_published.append(dspace_record)

        with open(self.to_upload, 'w') as f:
            json.dump(to_be_published, f, indent=4)
        with open(self.redirects, 'w') as f:
            json.dump(redirects_j, f, indent=4)

        # Check for records in OSTI but not DSpace
        dspace_handles = [record['handle'] for record in dspace_j]
        errors = [record for record in osti_j if redirects_j[record['doi']] not in dspace_handles]
        if len(errors) > 0:
            print(f"The following records were found on OSTI but not in DSpace (that" + 
                  " shouldn't happen). If they closely resemble records we are about to" +
                  " upload, please remove those records from from the upload process.")
            for error in errors:
                print(f"\t{error['title']}")

    def generate_contract_entry_form(self):
        """Create a CSV where a user can enter Sponsoring Organizations, DOE
         Contract, and Datatype, additional information required by OSTI
        """
        with open(self.to_upload) as f:
            to_upload_j = json.load(f)

        df = pd.DataFrame()
        df['DSpace ID'] = [item['id'] for item in to_upload_j]
        df['Issue Date'] = [[m['value'] for m in item['metadata'] if m['key'] == 'dc.date.issued'][0]
                            for item in to_upload_j]
        df['Title'] = [item['name'] for item in to_upload_j]
        df['Author'] = [';'.join([m['value'] for m in item['metadata'] if
                                  m['key'] == 'dc.contributor.author'])
                        for item in to_upload_j]
        df['Dataspace Link'] = ["https://dataspace.princeton.edu/handle/" + item['handle']
                                for item in to_upload_j]

        # To be filled in:
        df['Sponsoring Organizations'] = None
        df['DOE Contract'] = None
        df['Datatype'] = None

        df = df.sort_values('Issue Date')
        df.to_csv(self.entry_form, index=False, sep='\t')

        print(f"{df.shape[0]} unpublished records were found in the PPPL "
              f"dataspace community that have not been registered with OSTI.")
        print(f"They've been saved to the form {self.entry_form}.")
        print("You're now expected to manually update that form and save as a "
              "new file before running Poster.py")
        for i, row in df.iterrows():
            print(f"\t{repr(row['Title'])}")
            print(f"\t\t{row['Dataspace Link']}")

    def run_pipeline(self, scrape=True):
        if scrape:
            self.get_existing_datasets()
            self.get_dspace_metadata()
        self.get_unposted_metadata()
        self.generate_contract_entry_form()


if __name__ == '__main__':
    s = Scraper()
    # NOTE: It may be useful to implement a CLI command (e.g. --no-scrape) to 
    #  allow for debugging the get_unposted_metadata or generate_contract_entry_form
    #  functions
    s.run_pipeline()
