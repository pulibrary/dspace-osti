# dspace-osti 

For oversight reasons, [OSTI](https://www.osti.gov/) requires that PPPL submit its datasets' metadata through their API. OSTI is only a metadata repository, and the datasets themselves are stored in Dataspace. We are responsible for posting the metadata by the end of each fiscal year. This is not to be confused with submitting journal article metadata to OSTI, which is an entirely separate process and is handled by PPPL.

## Useful Links:

- [OSTI API](https://www.osti.gov/elink/241-6api.jsp)
- [Previously submitted PPPL datasets](https://www.osti.gov/dataexplorer/api/v1/records?site_ownership_code=PPPL)
- [PPPL DSpace Community](https://dataspace.princeton.edu/handle/88435/dsp01pz50gz45g)
- [DSpace REST Documentation](https://dataspace.princeton.edu/rest/)
- [OSTI's old code](https://github.com/doecode/dspace)

## Get an ELink account

To post to OSTI, a user needs to acquire an ELink account. Currently, the OSTI API does not have a UI to create an account, so a new user will have to OSTI directly.

## Setup

`ostiapi` looks for two environment variables, `OSTI_USERNAME` and `OSTI_PASSWORD`. After a user gets an ELink Account, one can set the appropriate variables in `secrets.sh`, which is already removed from version control by `.gitignore`.

```
export OSTI_USERNAME="my-osti-username"
export OSTI_PASSWORD="my-osti-password"
```

We are dependent on [ostiapi](https://github.com/doecode/ostiapi) as a submodule. Presumably, this will eventually be available on PyPi. For all other libraries install the requirements in a python 3.8 environment.

```
pip install -r requirements.txt
```

## Pipeline

### Pull necessary data

#### get_existing_datasets.py
Pull the JSON of datasets we've already pushed to OSTI in previous fiscal years. Output `existing_osti_datasets.json`

#### get_dspace_metadata.py
Pull the JSON of datasets we currently host on Dataspace. Output `dspace_datasets.json`

#### get_unposted_metadata.py
Find the difference between `existing_osti_datasets.json` and `dspace_datasets.json`, and output `dataset_metadata_to_upload.json`.


### Prepare for publishing
Some metadata required by OSTI is not found in the DSpace metadata. Therefore, a spreadsheeet needs to be filled out with the appropriate information.

#### generate_contract_entry_form.py
Generate `contract_info.csv` that can be used to collect federal contract numbers and sponsor organizations.

#### generate_upload_json.py
Given all data files mentioned above, create the final JSON, `osti.json`, which will be used to post to OSTI.


### Post to OSTI
#### post_to_osti.py
Use the OSTI API to post `osti.json`.

