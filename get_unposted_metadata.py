import json
import os
import html

data_dir = "data"
dspace = os.path.join(data_dir, "dspace_dataset_metadata.json")
osti = os.path.join(data_dir, "existing_osti_datasets.json")
to_upload = os.path.join(data_dir, "dataset_metadata_to_upload.json")

def get_unposted_metadata():
    """Compare the OSTI and DSpace JSON to see what titles need to be uploaded.
    """

    with open(dspace) as f:
        dspace_j = json.load(f)
    with open(osti) as f:
        osti_j = json.load(f)

    osti_titles = [html.unescape(x['title']) for x in osti_j]
    dspace_titles = [x['name'] for x in dspace_j]

    titles_to_be_published = [x for x in dspace_titles if x not in osti_titles]
    to_be_published = [item for item in dspace_j if item['name'] in titles_to_be_published]

    with open(to_upload, 'w') as f:
        json.dump(to_be_published, f, indent=4)

    print(f"{len(to_be_published)} unpublished records were found.", end="\n\n")
    

    errors = [x for x in osti_titles if x not in dspace_titles]
    if len(errors) > 0:
        print(f"The following records were found on OSTI but not in DSpace (that" + 
            " shouldn't happen). If they closely resemble records we are about to" +
            " upload, please remove those records from from the upload process.")
        for error in errors:
            print("---  " + error)

   

if __name__ == '__main__':
    get_unposted_metadata()