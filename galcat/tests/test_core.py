# Unit tests for core.py
import pytest
import astropy.units as u
from galcat.core import *

USE_MONGO = False


def setup_module(module):
    if USE_MONGO:
        # MongoDB version
        module.db = Database(conn_string='localhost', mongo_db_name='GalaxyCat',
                             collection_name='galaxies_test', references_file='test_references.json')
    else:
        # Local version
        module.db = Database(directory='test_data', references_file='test_references.json')


def test_load_database():
    assert not isinstance(db, type(None))


def test_query_db():
    doc = db.query_db({'name': 'Gal 2'})
    assert doc[0]['ra'][0]['value'] == 10.4

    doc = db.query_db({'name': 'I DONT EXIST'})
    assert len(doc) == 0

    doc = db.query_db({'fake_column': 5})
    assert len(doc) == 0

    # EBV has no error_upper. Fails locally but not on MongoDB
    docs = db.query_db({'ebv.error_upper': 0.5})
    assert len(docs) == 0


def test_operator_math():
    query = {'v_mag.value': {'$lt': 21}}
    docs = db.query_db(query)
    assert len(docs) == 2
    for elem in [s['v_mag'] for s in docs]:
        assert elem[0]['value'] < 21

    query = {'v_mag.value': {'$lte': 16.2}}
    docs = db.query_db(query)
    assert len(docs) == 1
    for elem in [s['v_mag'] for s in docs]:
        assert elem[0]['value'] <= 16.2

    query = {'v_mag.value': {'$gt': 10}}
    docs = db.query_db(query)
    assert len(docs) == 2
    for elem in [s['v_mag'] for s in docs]:
        assert elem[0]['value'] > 10

    query = {'v_mag.value': {'$gte': 20.2}}
    docs = db.query_db(query)
    assert len(docs) == 1
    for elem in [s['v_mag'] for s in docs]:
        assert elem[0]['value'] >= 20.2

    query = {'v_mag.value': {'$gte': 999}}
    docs = db.query_db(query)
    assert len(docs) == 0


def test_operator_exists():
    query = {'radial_velocity.value': {'$exists': True}}
    docs = db.query_db(query)
    assert len(docs) == 1
    assert docs[0]['name'] == 'Gal 1'

    # This fails locally, but not in MongoDB
    query = {'radial_velocity': {'$exists': True}}
    docs = db.query_db(query)
    assert len(docs) == 1
    assert docs[0]['name'] == 'Gal 1'

    # This fails locally, but not in MongoDB
    query = {'radial_velocity.value': {'$exists': False}}
    docs = db.query_db(query)
    print(docs)
    assert len(docs) == 1
    assert docs[0]['name'] == 'Gal 2'


def test_operator_or():
    query = {'v_mag.value': {'$gt': 10}, 'radial_velocity.value': {'$lte': -100}}
    docs = db.query_db(query)
    assert len(docs) == 1
    assert docs[0]['name'] == 'Gal 1'

    query = {'$or': [{'v_mag.value': 16.2}, {'dec.value': -32.4}]}
    docs = db.query_db(query)
    assert len(docs) == 2

    query = {'ra.value': {'$gt': 10}, '$or': [{'v_mag.value': {'$lte': 21}},
                                              {'v_mag.value': {'$gte': 16}}]}
    docs = db.query_db(query)
    assert len(docs) == 2

    query = {'ra.value': {'$gt': 10}, '$or': [{'v_mag.error_upper': {'$lte': 0.4}},
                                              {'v_mag.error_upper': {'$gte': 0.2}}]}
    docs = db.query_db(query)
    assert len(docs) == 2

    query = {'$or': [{'v_mag.value': 16.2}, {'light_radius.error_upper': 0.12}]}
    docs = db.query_db(query)
    assert len(docs) == 1


def test_query_table():
    df = db.query_table({'name': 'I DONT EXIST'})
    assert len(df) == 0

    df = db.query_table({'name': 'Gal 1'})
    assert df[['ra']][0][0].value == 9.14542
    assert df[['ra']][0][0].unit == u.deg

    df = db.query_table({'name': 'Gal 1'}, selection={'ra': 'FakeRef2019'})
    assert df[['ra']][0][0].value == 999.14542


def test_load_file_to_db():
    doc = {"name": "Gal 3",
           "ra": [{"value": 5, "best": 1, "reference": "", "unit": "deg"}],
           "dec": [{"value": -1, "best": 1, "reference": "", "unit": "deg"}],
           "ebv": [{"value": 0.2, "best": 1, "reference": "Bellazzini_2006_1"}]}
    db.load_file_to_db(doc)

    doc = db.query_db({'name': 'Gal 3'})
    assert len(doc) > 0
    assert doc[0]['ra'][0]['value'] == 5

    # Delete the added document if using MongoDB
    if USE_MONGO:
        db.db.delete_one({'name': 'Gal 3'})

