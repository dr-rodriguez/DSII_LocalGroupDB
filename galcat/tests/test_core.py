# Unit tests for core.py
import os
import numpy as np
import pytest
import astropy.units as u
from astropy import uncertainty as unc
from astropy.table import QTable
from galcat.core import *
from galcat.core import _get_values_from_distribution

USE_MONGO = False


def setup_module(module):
    if USE_MONGO:
        # MongoDB version
        module.db = Database(conn_string='localhost', mongo_db_name='GalaxyCat',
                             collection_name='galaxies_test', references_file='galcat/tests/test_references.json')
    else:
        # Local version
        module.db = Database(directory='galcat/tests/test_data', references_file='galcat/tests/test_references.json')


def test_load_database():
    assert not isinstance(db, type(None))


def test_query_db():
    doc = db.query_db({'name': 'Gal 2'})
    assert doc[0]['ra'][0]['value'] == 10.4

    doc = db.query_db({'name': 'I DONT EXIST'})
    assert len(doc) == 0

    doc = db.query_db({'fake_column': 5})
    assert len(doc) == 0

    doc = db.query({'name': 'Gal 1'}, embed_ref=True)
    assert doc[0]['ebv'][0]['reference']['bibcode'] == '2006MNRAS.366..865B'

    doc = db.query({'name': 'Gal 1'}, embed_ref=False)
    assert doc[0]['ebv'][0]['reference'] == 'Bellazzini_2006_1'

    # EBV has no error_upper so this should return nothing
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
    if USE_MONGO:
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
    else:
        with pytest.raises(RuntimeError):
            query = {'radial_velocity.value': {'$exists': True}}
            docs = db.query_db(query)


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

    query = {'$or': [{'v_mag.value': 16.2}, {'half-light_radius.error_upper': 0.12}]}
    docs = db.query_db(query)
    assert len(docs) == 1


def test_query_table():
    df = db.query_table({'name': 'I DONT EXIST'})
    assert len(df) == 0

    df = db.query_table({'name': 'Gal 1'})
    assert df[['ra']][0][0].value == 9.14542
    assert df[['ra']][0][0].unit == u.deg
    assert isinstance(df, QTable)

    df = db.query_table({'name': 'Gal 1'}, selection={'ra': 'FakeRef2019'})
    assert df[['ra']][0][0].value == 999.14542

    query = {'$or': [{'v_mag.value': 16.2}, {'half-light_radius.error_upper': 0.12}]}
    df = db.query_table(query)
    assert len(df) == 1


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


@pytest.mark.parametrize('test_input, result', [
    ((5, u.kg), 5*u.kg),
    ((5, 'penguin'), 5),
])
def test_store_quantity(test_input, result):
    assert db._store_quantity(*test_input) == result


def test_add_data():
    # reset DB values
    db.load_file_to_db('galcat/tests/test_data/Gal_1.json')

    docs = db.query_db({'name': 'Gal 1', 'fake_quantity.value': 1})
    assert len(docs) == 0

    # Missing name should through a RuntimeError
    with pytest.raises(RuntimeError):
        doc = {'fake_quantity': [{'value': 1}]}
        db.add_data(doc, update_value=False, validate=False)

    doc = {'name': 'Gal 1',
           'fake_quantity': [{'value': 1}]}
    db.add_data(doc, update_value=False, validate=False)

    # Confirm value got loaded
    docs = db.query_db({'name': 'Gal 1', 'fake_quantity.value': 1})
    assert len(docs) == 1

    # reset DB values
    db.load_file_to_db('galcat/tests/test_data/Gal_1.json')


def test_save_from_db(tmpdir):
    doc = db.query_db({'name': 'Gal 1'})[0]
    db.save_from_db(doc, out_dir=tmpdir)
    assert os.path.isfile(os.path.join(tmpdir, 'Gal_1.json'))


def test_recursive_json_fix():
    doc = {"name": "Gal 3",
           "ebv": [{"value": 0.2, "best": 1, "reference": "Bellazzini_2006_1"},
                   {"value": 0.4, "best": 0},
                   {"value": 0.6, "best": 0, "list": [{'a': 1}, {'a': 2}, {'a': 3}]}],
           "list2": [1, 2, 3]}
    newdoc = db._recursive_json_fix(doc)
    assert isinstance(newdoc['ebv'], np.ndarray)
    assert isinstance(newdoc['ebv'][2]['list'], np.ndarray)
    assert isinstance(newdoc['list2'], np.ndarray)
    assert len(newdoc['ebv']) == 3


def test_recursive_json_reverse_fix():
    doc = {"name": "Gal 3",
           "ebv": np.array([{"value": 0.2, "best": 1, "reference": "Bellazzini_2006_1"},
                           {"value": 0.4, "best": 0},
                           {"value": 0.6, "best": 0, "list": np.array([{'a': 1}, {'a': 2}, {'a': 3}])}]),
           "list2": np.array([1, 2, 3])}
    newdoc = db._recursive_json_reverse_fix(doc)
    assert isinstance(newdoc['ebv'], list)
    assert isinstance(newdoc['ebv'][2]['list'], list)
    assert isinstance(newdoc['list2'], list)
    assert len(newdoc['ebv']) == 3


def test_get_values_from_distribution():
    center, std = 1, 0.2
    np.random.seed(12345)
    n_distr = unc.normal(center * u.kpc, std=std * u.kpc, n_samples=100)
    result = _get_values_from_distribution(n_distr)
    assert center == pytest.approx(result['value'], abs=1e-2)
    assert std == pytest.approx(result['error'], abs=1e-2)
    assert 'kpc' == result['unit']

    np.random.seed(12345)
    n_distr = unc.normal(center, std=std, n_samples=100)
    result = _get_values_from_distribution(n_distr)
    assert center == pytest.approx(result['value'], abs=1e-2)
    assert std == pytest.approx(result['error'], abs=1e-2)
    assert result.get('unit', None) is None

    result = _get_values_from_distribution(n_distr, unit='kpc')
    assert 'kpc' == result['unit']

    distr = [1.20881063, 0.93766121, 1.20136033, 1.11122468, 0.88140548,
           0.98529047, 0.83750181, 0.95603778, 0.90262727, 0.76719971,
           0.96954131, 0.83957612, 1.05208742, 0.9203976 , 0.5388856 ,
           0.82028187, 0.99002746, 0.99821842, 1.08264829, 0.88236597,
           1.07393172, 0.68800062, 0.95087714, 0.95349601, 1.20331926,
           1.1427941 , 1.13346843, 1.12862014, 1.32770298]
    result = _get_values_from_distribution(distr)
    assert center == pytest.approx(result['value'], abs=5e-2)
    assert std == pytest.approx(result['error'], abs=5e-2)


def test_distribution():
    doc = {'name': 'Gal 9',
           'ebv': [{'distribution': [1.20881063, 0.93766121, 1.20136033, 1.11122468, 0.88140548,
           0.98529047, 0.83750181, 0.95603778, 0.90262727, 0.76719971,
           0.96954131, 0.83957612, 1.05208742, 0.9203976 , 0.5388856 ,
           0.82028187, 0.99002746, 0.99821842, 1.08264829, 0.88236597,
           1.07393172, 0.68800062, 0.95087714, 0.95349601, 1.20331926,
           1.1427941 , 1.13346843, 1.12862014, 1.32770298], 'reference': 'Fake'}]}
    db.load_file_to_db(doc)

    df = db.query_table({'name': 'Gal 9'})
    assert df[['ebv']][0][0] == pytest.approx(1, abs=5e-2)

    # Queries against distribution only work with MongoDB implementation
    if USE_MONGO:
        docs = db.query({'ebv.distribution': {'$lte': 1}})
        assert len(docs) == 1
        assert docs[0]['name'] == 'Gal 9'
        docs = db.query({'ebv.distribution': {'$gte': 10}})
        assert len(docs) == 0
        db.db.delete_one({'name': 'Gal 9'})

