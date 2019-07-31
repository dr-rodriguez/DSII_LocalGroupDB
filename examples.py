# Simple examples to test or demo things

from galcat.core import *
db = Database()

# Full database as a table of best values
df = db.table()
df.head()

# Example to save JSON data to file
db.save_from_db(db.db[0], verbose=True)
db.save_from_db(db.db[0], save=True)

# Simple query examples
db.query({'name': 'And XXX'})
db.query({'ra.value': 10.68458})

# Example query with operator
query = {'surface_brightness.value': {'$gt': 27}}
db.query(query=query)
df = db.table(query=query)
df[['name', 'surface_brightness']]

# Example AND query
query = {'surface_brightness.value': {'$gt': 27}, 'radial_velocity.value': {'$lte': -100}}
db.query(query=query)
df = db.table(query=query)
df[['name', 'surface_brightness', 'radial_velocity']]

# Example OR query
query = {'$or': [{'ra.value': 10.68458}, {'dec.value': 49.64667}]}
db.query(query=query)
df = db.table(query=query)
df[['name', 'ra', 'dec']]

# Example table output with requested selection (ra from reference FakeRef2019)
query = {'name': 'And XXX'}
db.query(query=query)
db.table(query=query)[['name', 'ra', 'dec']]
db.table(query=query, selection={'ra': 'FakeRef2019'})[['name', 'ra', 'dec']]

# TODO: Add entry to existing field
# TODO: Update entry
