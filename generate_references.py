# Script to read References.dat and generate a json document for the database


# Read References.dat
# This has references across multiple rows and in multiple formats.
# Will need to see if ADS can be used to get the unique reference automatically

# Should each reference be a separate JSON? They will be separate entries in the database, but it may be
# easier as a single file on disk

# Example JSON
"""
{
  "key": "Rodriguez_2019_1",
  "year": 2019,
  "doi": "10.1088/0004-637X/774/2/101",
  "bibcode": "2013ApJ...774..101R",
  "authors": ["Rodriguez, David R.", "Zuckerman, B.", "Kastner, Joel H.", "Bessell, M. S.", "Faherty, Jacqueline K.",
              "Murphy, Simon J."],
  "journal": "The Astrophysical Journal"
  "title": "The GALEX Nearby Young-Star Survey"
}

The absolute minimum will probably be just be the key (as author_year_integer), though year and authors may be useful
"""

# I can imaginge there being functions to populate the database from calls to ADS given the doi/bibcode
# However, I also think that queries to the reference table will not be required

