#!/usr/bin/env python

import json
import pandas as pd


if __name__ == "__main__":

    # Load OSTI data
    with open("data/redirects.json") as f:
        osti_data = json.load(f)

    df = pd.read_excel(
        "DataSpace Collections Migration Tracking.xlsx",
        sheet_name="Records",
        header=0,
        skiprows=[1, 2]
    )

    rev = []
    for i, row in df.iterrows():
        osti_match = [key for key, value in osti_data.items() if value == row["handle"]]
        if osti_match:
            rev.append(osti_match[0])
        else:
            rev.append("")

    df.DOI = rev
    df.to_excel("data/dspace_doi.csv", index=False)
