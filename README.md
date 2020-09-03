# dspace-osti 

For oversight reasons, [OSTI](https://www.osti.gov/) requires that PPPL submit its datasets' metadata through their API. OSTI is only a metadata repository, and the datasets themselves are stored in Dataspace. We are responsible for posting the metadata by the end of each fiscal year. This is not to be confused with submitting journal article metadata to OSTI, which is an entirely separate process and is handled by PPPL.

## Setup

### Get an ELink account

To post to OSTI, a user needs to acquire an ELink account. Currently, the OSTI API does not have a UI to create an account, so a new user will have to OSTI directly.

### Setup an environment
We are dependent on [ostiapi](https://github.com/doecode/ostiapi) as a submodule. Presumably, this will eventually be available on PyPi. For all other libraries install the requirements in a python 3.8 environment.

```
pip install -r requirements.txt
```

`ostiapi` looks for two environment variables, `OSTI_USERNAME` and `OSTI_PASSWORD`. After a user gets an ELink Account, one can set the appropriate variables in `secrets.sh`, which is already removed from version control by `.gitignore`.

```
export OSTI_USERNAME="my-osti-username"
export OSTI_PASSWORD="my-osti-password"
```

## Workflow

### Pull necessary data

Run `python Scraper.py` to collect data from OSTI & DSpace. The pipeline will compare (by title) to see which datasets haven't yet been uploaded. It will output `entry_form.csv` that one needs to manually fill out with DOE Contract information 

### Manually enter data

Copy `entry_form.csv` to a Google Sheet and share with partners at PPPL. They will need to enter `Sponsoring Organizations`, `DOE Contract`, and `Datatype`. [See `Datatype` codes here.](https://github.com/doecode/ostiapi#data-set-content-type-values) (Ideally, in the long run we would integrate these fields into DSpace's metadata.) Save the file into this folder as `form_input.csv`.

Note: Since we're joining by title, typos and encoding errors will inevitably lead to missed results. `Scraper.py` also checks for items that are in OSTI but not DSpace, something that shouldn't happen. The user will need to manually remove those rows from the entry form.

### Post to OSTI

Run `python Poster.py` to use the `form_input.csv` and DSpace metadata to generate the JSON necessary for OSTI ingestion. It then posts to OSTI using their API.


## Useful Links:

- [OSTI API](https://www.osti.gov/elink/241-6api.jsp)
- [Previously submitted PPPL datasets](https://www.osti.gov/dataexplorer/api/v1/records?site_ownership_code=PPPL)
- [PPPL DSpace Community](https://dataspace.princeton.edu/handle/88435/dsp01pz50gz45g)
- [DSpace REST Documentation](https://dataspace.princeton.edu/rest/)
- [OSTI's old code](https://github.com/doecode/dspace)
