# Script to read References.dat and generate a json document for the database


# Read References.dat
# This has references across multiple rows and in multiple formats.
# Will need to see if ADS can be used to get the unique reference automatically

# Should each reference be a separate JSON? They will be separate entries in the database, but it may be
# easier as a single file on disk

# Example JSON
"""
[
  {
    "key": "Bellazzini_2006_1",
    "id": 1,
    "year": 2006,
    "doi": "",
    "bibcode": "",
    "authors": [
      "Bellazzini, M.",
      "Ibata, R.",
      "Martin, N.",
      "Lewis, G. F.",
      "Conn, B.",
      "Irwin, M. J."
    ],
    "journal": "MNRAS",
    "title": "The core of the Canis Major galaxy as traced by red clump stars"
  },
  {
    "key": "Martin_2005_1",
    "id": 2,
    "year": 2005,
    "doi": "",
    "bibcode": "",
    "authors": [
      "Martin, N. F.",
      "Ibata, R. A.",
      "Conn, B. C.",
      "Lewis, G. F.",
      "Bellazzini, M.",
      "Irwin, M. J."
    ],
    "journal": "MNRAS",
    "title": "A radial velocity survey of low Galactic latitude structures - I. Kinematics of the Canis Major dwarf galaxy"
  }
]

The absolute minimum will probably be just be the key (as author_year_integer), though year and authors may be useful
"""

# I can imagine there being functions to populate the database from calls to ADS given the doi/bibcode
# However, I also think that queries to the reference table will not be required

import json
import requests
import urllib.parse
import ads
from dsii_secrets import ads_api_key

# Example query
papers = list(ads.SearchQuery(q="The core of the Canis Major galaxy as traced by red clump stars"))
papers = list(ads.SearchQuery(author="Bellazzini", year=2006))
papers = list(ads.SearchQuery(first_author="Bellazzini", year=2006,
                              q='The core of the Canis Major galaxy as traced by red clump stars'))

for p in papers:
    print(p.year, p.author[0], p.title, p.bibcode, p.doi)

# Bellazzini_2006_1
# 10.1111/j.1365-2966.2005.09973.x
# 2006MNRAS.366..865B

# Get references data
with open('references.json', 'r') as f:
    refs = json.load(f)

for ref in refs:
    if ref.get('bibcode', '') == '' or ref.get('doi', '') == '':
        author = ref.get('authors', [])[0]
        year = ref.get('year')
        title = ref.get('title')

        papers = list(ads.SearchQuery(first_author=author, year=year, title=title))
        if len(papers) == 1:
            p = papers[0]

            ref['bibcode'] = p.bibcode
            ref['doi'] = p.doi[0]

            print(ref)
        elif len(papers) > 1:
            print('Warning: more than one paper matched')
            for p in papers:
                print(p.year, p.author[0], p.title, p.bibcode, p.doi)

# Output to file
out_refs = json.dumps(refs, indent=2, sort_keys=False)
with open('references.json', 'w') as f:
    f.write(out_refs)
