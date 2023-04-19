#!/usr/bin/env python

import json
import requests
import pandas as pd

from Scraper import PPPL_COLLECTIONS


def make_dict(data: dict, collection_name: str, doi: str = "", osti_id: str = ""):
    m = data.get("metadata")
    j_dict = {
        "DSpace ID": data.get("id"),
        "ARK": data.get("handle"),
        "DOI": doi,
        "OSTI ID": osti_id,
        "Issue Date": [k["value"] for k in m if k["key"] == "dc.date.issued"][0],
        "Collection": collection_name,
        "Author": [
            ";".join([k["value"] for k in m if k["key"] == "dc.contributor.author"])
        ][0],
        "Title": data.get("name"),
        "DataSpace URL": f"https://dataspace.princeton.edu/handle/{data.get('handle')}",
    }
    return j_dict


content_audit_columns = [
    "DSpace ID",
    "ARK",
    "DOI",
    "OSTI ID",
    "Issue Date",
    "Collection",
    "Author",
    "Title",
    "DataSpace URL",
]


if __name__ == "__main__":

    # Load OSTI data
    with open("data/redirects.json") as f:
        osti_data = json.load(f)

    df = pd.DataFrame(columns=content_audit_columns)

    for c_name, c_id in PPPL_COLLECTIONS.items():
        url = f"https://dataspace.princeton.edu/rest/collections/{c_id}/items?expand=metadata"
        r = requests.get(url)
        j = json.loads(r.text)

        rev = []
        for item in j:
            osti_match = [key for key, value in osti_data.items() if value == item.get("handle")]
            osti_doi = osti_match[0].replace("https://doi.org/", "") if osti_match else ""
            rev.append(make_dict(item, c_name, doi=osti_doi, osti_id=osti_doi.replace("10.11578/", "")))
        df = pd.concat([df, pd.DataFrame.from_records(rev)], ignore_index=True)

    df.sort_values(by="DSpace ID", inplace=True)
    df.to_csv("data/dspace_audit.csv", index=False)
