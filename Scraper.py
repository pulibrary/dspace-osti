import json
import os
import re
from typing import Dict

import requests
import pandas as pd

from os.path import join as pjoin

DSPACE_ID = 'DSpace ID'

# NOTE: The Dataspace REST API can now support requests from handles.
#  Shifting this scrape to collection handles instead of IDs may make
#  this script clearer and easier to change if necessary.
PPPL_COLLECTIONS = {
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
    'IT PPPL Collaborations': 3383,
    'Advanced Projects Other Projects': 3386,
    'Advanced Projects System Studies': 1309,
}

PPPL_COMMUNITY_ID = 346

# All possible prefix
REGEX_DOE = r"^(DE|AC|SC|FC|FG|AR|EE|EM|FE|NA|NE)"  # https://regex101.com/r/SxNHJg
REGEX_DOE_SUB = "^(DE)+(-?)"  # https://regex101.com/r/NsZbRJ
REGEX_FUNDING = r"\b(?:[A-Z0-9\/\-]{6,})"  # https://regex101.com/r/4fNYVm
REGEX_BARE_DOE = re.compile(
    r"(^((U.S.|U. S.) (Department of Energy))|FES)$"
)  # https://regex101.com/r/2s3dA3

REPLACE_DICT = {
    '- ': '-',  # Extra white space inside DoE grant
    'AC02 ': 'AC02-',  # Missing hyphen
    'AC-02': 'AC02',  # Extra hyphen
    'SC-0': 'SC0',  # Extra hyphen for Office of Science grants
    'DC': 'DE',  # Common typo
    'DE ': 'DE',  # Extra white space
    'DOE-': 'DE',  # Proper prefix
    'DOE ': '',  # Extra DOE
    'DOE': '',  # Remove DOE if still present
}


class Scraper:
    """
    Pipeline to collect data from OSTI & DataSpace, comparing which datasets
    are not yet posted, and generating a form for a user to manually enter
    additional needed information

    :param data_dir: Local data folder for save files
    :param osti_scrape: JSON output file containing OSTI metadata
    :param dspace_scrape: JSON output file containing DataSpace metadata
    :param entry_form_full_path: TSV file containing DataSpace
           records not in OSTI
    :param form_input_full_path: TSV file containing DataSpace
           records and DOE metadata for submission
    :param to_upload: JSON output file containing metadata for OSTI upload
    :param redirects: JSON output file containing DOI redirects

    :ivar osti_scrape: JSON output file containing OSTI metadata
    :ivar dspace_scrape: JSON output file containing DataSpace metadata
    :ivar entry_form: TSV file containing DataSpace records not in OSTI
    :ivar to_upload: JSON output file containing metadata for OSTI upload
    :ivar redirects: JSON output file containing DOI redirects
    """
    def __init__(self, data_dir='data', osti_scrape='osti_scrape.json',
                 dspace_scrape='dspace_scrape.json',
                 entry_form_full_path='entry_form.tsv',
                 form_input_full_path='form_input.tsv',
                 to_upload='dataset_metadata_to_upload.json',
                 redirects='redirects.json'):

        self.osti_scrape = pjoin(data_dir, osti_scrape)
        self.dspace_scrape = pjoin(data_dir, dspace_scrape)
        self.entry_form = entry_form_full_path
        self.form_input = form_input_full_path
        self.to_upload = pjoin(data_dir, to_upload)
        self.redirects = pjoin(data_dir, redirects)

        if not os.path.exists(data_dir):
            os.mkdir(data_dir)

    def get_existing_datasets(self):
        """
        Paginate through OSTI's Data Explorer API to find datasets that have
        been submitted
        """
        MAX_PAGE_COUNT = 15
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
        """
        Collect metadata on all items from all DataSpace PPPL collections

        """

        all_items = []

        for c_name, c_id in PPPL_COLLECTIONS.items():
            url = f'https://dataspace.princeton.edu/rest/collections/{c_id}/items?expand=metadata'
            r = requests.get(url)
            j = json.loads(r.text)
            all_items.extend(j)

        # Confirm that all collections were included
        url_all = f"https://dataspace.princeton.edu/rest/communities/{PPPL_COMMUNITY_ID}"
        r = requests.get(url_all)
        print('countItems: ', json.loads(r.text)['countItems'])
        print('all_items: ', len(all_items))
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
        """Compare OSTI and DataSpace JSON to identify records to be uploaded"""
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
        """
        Create a CSV where a user can enter Sponsoring Organizations, DOE
        Contract, and Datatype, additional information required by OSTI
        """
        with open(self.to_upload) as f:
            to_upload_j = json.load(f)

        df = pd.DataFrame()
        df[DSPACE_ID] = [item['id'] for item in to_upload_j]
        df['Issue Date'] = [[m['value'] for m in item['metadata'] if m['key'] == 'dc.date.issued'][0]
                            for item in to_upload_j]
        df['Title'] = [item['name'] for item in to_upload_j]
        df['Author'] = [';'.join([m['value'] for m in item['metadata'] if
                                  m['key'] == 'dc.contributor.author'])
                        for item in to_upload_j]
        df['Dataspace Link'] = ["https://dataspace.princeton.edu/handle/" + item['handle']
                                for item in to_upload_j]

        # Retrieve funding data
        funding_text_list = [
            [
                m['value']
                for m in item['metadata']
                if m['key'] == 'dc.contributor.funder'
            ]
            for item in to_upload_j
        ]

        # Generate lists of lists per each dc.contributor.funder entry
        funding_result = [
            list(filter(None, map(get_funder, f_list)))
            for f_list in funding_text_list
        ]
        funding_result_simple = [  # All grants for each DSpace record
            ";".join([";".join(value) if value else "" for value in res])
            for res in funding_result
        ]
        funding_source_dict = list(map(get_doe_funding, funding_result_simple))

        df['DOE Contract'] = [
            ";".join(sorted(d.get("doe"))) for d in funding_source_dict
        ]
        df['Non-DOE Contract'] = [
            ";".join(sorted(d.get("other"))) for d in funding_source_dict
        ]

        # Sponsoring organizations is always Office of Science
        df['Sponsoring Organizations'] = "USDOE Office of Science (SC)"

        df['Datatype'] = None  # To be filled in

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

    def update_form_input(self):
        """
        Update form_input.tsv by adding new records or removing DataSpace
        records that were removed/withdrawn

        In most cases, this will update form_input.tsv. This further supports CI
        """
        if os.path.exists(self.form_input):
            print(f"File exists. Will update: {self.form_input}")

            entry_df = pd.read_csv(self.entry_form, index_col=DSPACE_ID, sep='\t')
            input_df = pd.read_csv(self.form_input, index_col=DSPACE_ID, sep='\t')
            print("Identifying DataSpace records to add and remove ...")
            entry_id = set(entry_df.index)
            input_id = set(input_df.index)
            drops = input_id - entry_id
            adds = entry_id - input_id
            commons = entry_id & input_id
            print(f"Commons records : {len(commons):3}")
            print(f"New records     : {len(adds):3}")
            print(f"Records to drop : {len(drops):3}")
            print(f"Removing : {','.join([str(drop) for drop in drops])} ...")
            input_df.drop(drops, inplace=True)
            print(f"Appending : {','.join([str(add) for add in adds])}")
            revised_df = input_df.append(entry_df.loc[adds])

            # "AS" is a placeholder - not included in DataSpace metadata
            revised_df.loc[adds, 'Datatype'] = "AS"

            revised_df.to_csv(self.form_input, sep='\t')
        else:
            raise FileNotFoundError(f"WARNING: {self.form_input} does not exist!")

    def run_pipeline(self, scrape=True):
        if scrape:
            self.get_existing_datasets()
            self.get_dspace_metadata()
        self.get_unposted_metadata()
        self.generate_contract_entry_form()
        self.update_form_input()


def get_funder(text: str) -> list:
    """Aggregate funding grant numbers from text"""

    # Clean up text by fixing any whitespace to get full grant no.
    for key, value in REPLACE_DICT.items():
        text = text.replace(key, value)

    for hyphen in ["\u2010", "\u2013"]:
        text = text.replace(hyphen, '-')

    base_match = re.match(REGEX_BARE_DOE, text)
    if base_match:  # DOE/FES funded but no grant number
        return ["AC02-09CH11466"]
    else:
        matches = re.finditer(REGEX_FUNDING, text)
        return [m.group() for m in matches]


def get_doe_funding(grant_nos: str) -> Dict[str, set]:
    """Separate DOE from other funding. Prefix DE prefix"""

    grant_dict = {
        "doe": set(),
        "other": set(),
    }

    if not grant_nos:  # Empty case
        grant_dict["doe"].update(["AC02-09CH11466"])
    else:
        grants = grant_nos.split(";")
        for grant in grants:
            if re.match(REGEX_DOE, grant):
                grant_dict["doe"].update([re.sub(REGEX_DOE_SUB, "", grant)])
            else:
                grant_dict["other"].update([grant])

    return grant_dict


if __name__ == '__main__':
    s = Scraper()
    # NOTE: It may be useful to implement a CLI command (e.g. --no-scrape) to
    #  allow for debugging the get_unposted_metadata or generate_contract_entry_form
    #  functions
    s.run_pipeline()
