# Simple examples to test or demo things
from operator import itemgetter
from galcat.core import *

# Load the database
db = Database()

# All db content
db.db

# Full database as a table of best values
# This is now QTable so df.head() doesn't work
df = db.table()
df

# Simple query examples
db.query({'name': 'And XXX'})
db.query({'name': 'And XXX'}, embed_ref=True)  # embed reference dict in place of the reference key
db.query({'ra.value': 10.68458})

# Example to save JSON data to file
doc = db.query({'name': 'And XXX'})[0]
db.save_from_db(doc, verbose=True)
db.save_from_db(doc, save=True)

# Example query with operators ($)
query = {'surface_brightness.value': {'$gt': 27}}
db.query(query=query)
df = db.table(query=query)
df[['name', 'surface_brightness']]

# Example $exists query
query = {'stellar_radial_velocity_dispersion.value': {'$exists': True}}
db.query(query=query)
db.table(query=query)[['name', 'stellar_radial_velocity_dispersion']]

# Example AND query
query = {'surface_brightness.value': {'$gt': 27}, 'radial_velocity.value': {'$lte': -100}}
db.query(query=query)
db.table(query=query)[['name', 'surface_brightness', 'radial_velocity']]

query = {'surface_brightness.value': {'$gt': 27}, 'surface_brightness.error_upper': {'$lte': 0.5}}
db.query(query=query)
db.table(query=query)[['name', 'surface_brightness']]

# Example OR query
query = {'$or': [{'ra.value': 10.68458}, {'dec.value': 49.64667}]}
db.query(query=query)
df = db.table(query=query)
df[['name', 'ra', 'dec']]

query = {'surface_brightness.value': {'$gt': 27}, '$or': [{'surface_brightness.error_upper': {'$lte': 0.5}},
                                                          {'surface_brightness.error_lower': {'$gte': 1.0}}]}
db.table(query=query)[['name', 'surface_brightness']]
t = db.query(query=query)
[x['surface_brightness'] for x in t]
keys = ['name', 'surface_brightness']
[list(itemgetter(*keys)(x)) for x in t]

# Example table output with requested selection (ra from reference FakeRef2019)
query = {'name': 'And XXX'}
db.query(query=query)
db.table(query=query)[['name', 'ra', 'dec']]
db.table(query=query, selection={'ra': 'FakeRef2019'})[['name', 'ra', 'dec']]

# Query references table
db.query_reference({'id': 1})
db.query_reference({'key': 'Bellazzini_2006_1'})[0]

# TODO: Add entry to existing field
# TODO: Update entry

# Using mongodb
# If localhost you must be running a local mongodb server
from galcat.core import *
db = Database(conn_string='localhost', mongo_db_name='GalaxyCat', collection_name='galaxies')

# If no database exists, can create it from the directory (will also update existing documents)
# db.load_all('data')
# Update references, if needed
# db.update_references_mongodb('references.json')

# All queries from above (should) work the same in MongoDB
doc = db.query({'name': 'And XXX'})[0]
db.save_from_db(doc, verbose=True)
