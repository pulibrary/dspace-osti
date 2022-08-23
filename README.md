# dspace-osti 

For oversight reasons, [OSTI](https://www.osti.gov/) requires that PPPL submit its datasets' metadata through their API. OSTI is only a metadata repository, and the datasets themselves are stored in Dataspace. We are responsible for posting the metadata by the end of each fiscal year. This is not to be confused with submitting journal article metadata to OSTI, which is an entirely separate process and is handled by PPPL.

## Setup

### Get an ELink account

To post to OSTI's test server, a user needs to acquire an ELink account. Currently, the OSTI API does not have a UI to create an account, so a new user will have to contact OSTI directly. To post to the production account, go to LastPass and get Princeton's credentials.

### Setup an environment
We are dependent on [ostiapi](https://github.com/doecode/ostiapi) as a submodule. Presumably, this will eventually be available on PyPi. For all other libraries install the requirements in a python 3.8 environment.

```
pip install -r requirements.txt
```

`ostiapi` requires a username and a password, which are different for posting to either `test` or `prod`.
`Poster.py` searches for two environment variables for the appropriate `mode` (`test`/`prod`).
After a user gets an E-Link Account, one can set the appropriate variables in `secrets.sh`,
which is already removed from version control by `.gitignore`.

```
export OSTI_USERNAME_TEST="my-test-osti-username"
export OSTI_PASSWORD_TEST="my-test-osti-password"
export OSTI_USERNAME_PROD="my-prod-osti-username" # from LastPass
export OSTI_PASSWORD_PROD="my-prod-osti-password" # from LastPass
```

## Workflow

### Pull necessary data

Run `python Scraper.py` to collect data from OSTI & DSpace. The pipeline will compare (by title) to see which datasets haven't yet been uploaded. It will output `entry_form.tsv` that one needs to manually fill out with DOE Contract information 

### Manually enter data

Copy `entry_form.tsv` to a Google Sheet and share with partners at PPPL. They will need to enter `Datatype`. [See `Datatype` codes here.](https://github.com/doecode/ostiapi#data-set-content-type-values)
(Ideally, in the long run we would integrate these fields into DSpace's metadata.) Save the file into this folder as `form_input.tsv`.
The `Sponsoring Organization`, `DOE Contract` and `Non-DOE Contract` may need to be modified. The latter two are retrieved from DataSpace metadata.
Note that the default `Sponsoring Organization` is "USDOE Office of Science (SC)".

Note: Since we're joining by title, typos and encoding errors will inevitably lead to missed results in `entry_form.tsv`. `Scraper.py` also checks for items that are in OSTI but not DSpace, something that shouldn't happen. The user will need to manually remove those rows from the entry form.

### Post to OSTI

`Poster.py` is used to combine the `form_input.tsv` and DSpace metadata to generate the JSON necessary for OSTI ingestion. Choose one of the three options:

```
    --dry-run: Make fake requests locally to test workflow.
    --test: Post to OSTI's test server.
    --prod: Post to OSTI's prod server.
```

| :warning:  | Posting to OSTI, both through test and prod, will send an email to you, your team, and OSTI. Make sure that `data/osti.json` is in good shape by running `python Poster.py --dry-run` before posting with `--test`. After OSTI approves what you've posted to their test server, post to production with the `--prod` flag. Ideally, you'd only need to go through this process once.      |
|---------------|:------------------------|

### Examples
If you're confused about how the output of `Scraper.py` turns into the input for `Poster.py`, consider looking at the CSVs in the `examples` folder.

Also, successful runs of `Poster.py` will give the following output:
```
Posting data...
    ✔ Toward fusion plasma scenario planning
    ✔ MHD-blob correlations in NSTX
Congrats 🚀 OSTI says that all records were successfully uploaded!
```

## Useful Links:

- [OSTI API](https://www.osti.gov/elink/241-6api.jsp)
- [Previously submitted PPPL datasets](https://www.osti.gov/dataexplorer/api/v1/records?site_ownership_code=PPPL)
- [PPPL DSpace Community](https://dataspace.princeton.edu/handle/88435/dsp01pz50gz45g)
- [DSpace REST Documentation](https://dataspace.princeton.edu/rest/)
- [OSTI's old code](https://github.com/doecode/dspace)
