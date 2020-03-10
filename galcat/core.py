# Core functionality for database implementation
import os
import json
import warnings
import numpy as np
import pandas as pd
from copy import deepcopy
from astropy import units as u
from astropy.table import QTable, Table
from astropy.units import Quantity
from astropy.coordinates import SkyCoord
from astropy import uncertainty as unc

__all__ = ['Database', 'write_curation']


def _get_values_from_distribution(distribution, unit=None):
    """Assuming a normal distribution, return value+error; includes unit if provided"""

    if isinstance(distribution, (list, np.ndarray)):
        distribution = unc.Distribution(distribution)

    val = distribution.pdf_mean()
    err = distribution.pdf_std()

    if isinstance(val, u.quantity.Quantity):
        unit = val.unit.to_string()
        val = val.value
        err = err.value

    out_dict = {'value': val, 'error': err}
    if unit is not None:
        out_dict['unit'] = unit

    return out_dict


def _read_curation(curation):
    """
    Read a curation JSON to a dictionary

    Parameters
    ----------
    curation : str
        Name of curation JSON to read
    Returns
    -------
    curation_dict : dict
        Output of curation in dictionary form
    """

    curation_dict = {}
    if isinstance(curation, str) and os.path.exists(curation):
        with open(curation, 'r') as f:
            curation_dict = json.load(f)
    elif isinstance(curation, dict):
        curation_dict = curation.copy()

    return curation_dict


def write_curation(curation, existing={}, filename='curation.json'):
    """
    Output curation dictionary to a JSON file. Will append and write over an existing curation if provided.

    Parameters
    ----------
    curation : dict
        Curation dictionary you want to write out
    existing : dict or str
        File or dictionary for an existing dictionary to use a baseline
    filename : str
        Name of curation to use
    """

    final_dict = _read_curation(existing)
    final_dict.update(curation)

    with open(filename, 'w') as f:
        out_json = json.dumps(final_dict, indent=4, sort_keys=False)
        f.write(out_json)


class Database(object):
    def __init__(self, directory='data', conn_string='', mongo_db_name='', collection_name='',
                 references_file='references.json', references_collection='references'):
        """
        Database connection object which will prepare or load a database.
        It also includes a collection of references.

        Parameters
        ----------
        directory
        conn_string
        mongo_db_name
        collection_name
        references_file
        references_collection
        """

        # Load or establish connection
        self.use_mongodb = False

        if conn_string and mongo_db_name and collection_name:
            # Connect to mongoDB
            try:
                import pymongo
                self.use_mongodb = True
                client = pymongo.MongoClient(conn_string)
                database = client[mongo_db_name]  # database
                self.references = database[references_collection]
                self.db = database[collection_name]  # collection
            except ImportError:
                print('ERROR : pymongo package required for using MongoDB')
                self.use_mongodb = False
        else:
            self.db = np.array([])
            if not os.path.exists(references_file):
                msg = 'ERROR: A json file of references must be provided.'
                print(msg)
                raise RuntimeError(msg)
            with open(references_file, 'r') as f:
                self.references = json.load(f)
            self.load_all(directory)

    def load_all(self, directory):
        for filename in os.listdir(directory):
            # Skip hidden and non-json files
            if filename.startswith('.') or not filename.endswith('.json'):
                continue

            self.load_file_to_db(os.path.join(directory, filename))

    def load_file_to_db(self, filename, id_column='name'):
        """
        Load JSON file to database. If the document already exists (as matched by id_column), it gets updated.

        Parameters
        ----------
        filename : str or dict-like
            Name of JSON file to add. If a dict-like, it is treated as ready to load.
        id_column : str
            Name of field to use for matching (Default: 'name')
        """

        if isinstance(filename, str):
            with open(filename, 'r') as f:
                doc = json.load(f)
        else:
            doc = filename

        if self.use_mongodb:
            self.load_to_mongodb(doc, id_column=id_column)
        else:
            doc = self._recursive_json_fix(doc)
            # Check if already present and if so update, otherwise add as new
            name = doc.get(id_column, '')
            orig_doc = self.query_db({id_column: name})
            if len(orig_doc) > 0 and orig_doc[0][id_column] == name:
                ind = np.where(self.db == orig_doc[0])
                self.db[ind] = doc
            else:
                self.db = np.append(self.db, doc)

    def load_to_mongodb(self, doc, id_column='name'):
        # Load JSON file to MongoDB

        # Use name as the unique ID (may want to change later)
        id_value = doc[id_column]

        # Revert arrays to lists to make correct JSON documents
        doc = self._recursive_json_reverse_fix(doc)

        # This uses replace_one to replace any existing document that matches the filter.
        # If none is matched, upsert=True creates a new document.
        result = self.db.replace_one(filter={id_column: id_value}, replacement=doc, upsert=True)

    def update_references_mongodb(self, references_file, id_column='key'):
        """
        Method to load references from a provided file to the MongoDB database

        Parameters
        ----------
        references_file : str
            Name of references JSON file to load
        id_column : str
            Name of ID column to use to match against existing documents (default: key)
        """

        with open(references_file, 'r') as f:
            references = json.load(f)

        for doc in references:
            id_value = doc[id_column]
            result = self.references.replace_one(filter={id_column: id_value}, replacement=doc, upsert=True)

    def _recursive_json_fix(self, doc):
        """
        Recursively fix a JSON document to convert lists to numpy arrays.
        This is needed for queries against the MongoDB database.

        Parameters
        ----------
        doc : dict
            Document result from a query against a MongoDB database

        Returns
        -------
        out_doc : dict
            Fixed document
        """

        out_doc = {}

        if isinstance(doc, list):
            # Handle lists by converting to numpy arrays
            out_doc = np.array([])
            for elem in doc:
                if isinstance(elem, dict):
                    elem = self._recursive_json_fix(elem)
                out_doc = np.append(out_doc, elem)
        elif isinstance(doc, dict):
            # Handle dicts by recursively fixing
            for key, val in doc.items():
                if isinstance(val, dict):
                    out_doc[key] = self._recursive_json_fix(val)
                elif isinstance(val, list):
                    new_array = np.array([])
                    for elem in val:
                        new_val = self._recursive_json_fix(elem)
                        new_array = np.append(new_array, new_val)
                    out_doc[key] = new_array
                else:
                    out_doc[key] = val
        else:
            out_doc = doc

        return out_doc

    def _recursive_json_reverse_fix(self, doc):
        """
        Undo the work from _recursive_json_fix; that is turn arrays to list to make correct JSON documents.
        This is needed since MongoDB does not understand numpy arrays.

        Parameters
        ----------
        doc : dict
            Document result to convert

        Returns
        -------
        out_doct : dict
            Fixed document
        """

        out_doc = {}

        # Remove _id if present (used in MongoDB)
        if self.use_mongodb and isinstance(doc, dict) and '_id' in doc.keys():
            del doc['_id']

        if isinstance(doc, list):
            # Handle np.arrays by converting to list
            out_doc = []
            for elem in doc:
                if isinstance(elem, dict):
                    elem = self._recursive_json_reverse_fix(elem)
                out_doc.append(elem)
        elif isinstance(doc, dict):
            for key, val in doc.items():
                if isinstance(val, dict):
                    out_doc[key] = self._recursive_json_reverse_fix(val)
                elif isinstance(val, type(np.array([]))):
                    new_array = []
                    for elem in val:
                        new_val = self._recursive_json_reverse_fix(elem)
                        new_array.append(new_val)
                    out_doc[key] = new_array
                else:
                    out_doc[key] = val
        else:
            out_doc = doc

        return out_doc

    def save_from_db(self, doc, verbose=False, out_dir='', save=True, name=''):
        """
        Save a JSON representation of the document. Useful for exporting database contents.

        Parameters
        ----------
        doc :
            Document result from a query
        verbose : bool
            Flag to indicate whether the JSON representation should be printed in the terminal (Default: False)
        out_dir : str
            Directory to save JSON file (Default: '')
        save : bool
            Flag to indicate if the JSON representation should be saved (Default: True)
        name : str
            Name of output JSON file. If none is provided, the 'name' field is used to name it. (Default: '')
        """

        # Save a JSON representation
        out_doc = self._recursive_json_reverse_fix(doc)
        out_json = json.dumps(out_doc, indent=4, sort_keys=False)
        if verbose:
            print(out_json)
        if save:
            if not name:
                name = doc['name']
                name = name.strip().replace(' ', '_') + '.json'
            filename = os.path.join(out_dir, name)
            print(filename)
            with open(filename, 'w') as f:
                f.write(out_json)
        return

    def save_all(self, out_dir=''):
        # Save entire database to disk
        doc_list = self.query_db({})
        for doc in doc_list:
            self.save_from_db(doc, out_dir=out_dir, save=True)

    def add_data(self, filename, force=False, id_column='name', auto_save=False, save_dir='data', update_value=False,
                 validate=True):
        """
        Add JSON data to database. May need to use save_all() afterwards to explicitly save changes to disk.

        Parameters
        ----------
        filename : str or dict-like
            File name of JSON data to load. Alternatively, it can be the dict-like data you want to load.
        force : bool
            Flag to ignore validation (Default: False)
        id_column : str
            Field to use when matching names (Default: 'name')
        auto_save : bool
            Flag to trigger automatically saving (Default: False)
        save_dir : str
            Directory to use if auto-saving (Default: 'data')
        update_value : bool
            Flag to indicate whether or not to update values from duplicated references (Default: False)
        validate : bool
            Flag to run JSON validation (Default: True)
        """

        if isinstance(filename, str):
            with open(filename, 'r') as f:
                new_data = json.load(f)
        else:
            new_data = filename

        # Run validation
        if validate:
            from .validator import Validator
            v = Validator(database=self, db_object=new_data, is_data=True)
            if not v.run():
                print('Failed validation: {}'.format(filename))
                return

        name = new_data.get(id_column)
        if name is None:
            raise RuntimeError('JSON data is missing name information for field: {}'.format(id_column))

        # Get existing data that will be updated
        old_doc = self.query_db({id_column: name})[0]
        if len(old_doc) == 0:
            print('{} does not exist in the database! Use load_file_to_db() to load new objects.'.format(name))
            return

        # Loop through the new data, adding it all to old_doc
        for k, v in new_data.items():
            if k == id_column:
                continue

            old_values = old_doc.get(k)
            if old_values is None:
                old_doc[k] = np.array(v)
            else:
                ref_list = [x['reference'] for x in old_doc[k]]
                # Loop over all new entries to be inserted, checking references at each stage
                for i in range(len(v)):
                    ref = v[i]['reference']

                    if ref in ref_list:
                        ind = np.where(np.array(ref_list) == ref)
                        print('Duplicate reference for {} found: {}. Values: {}'.format(k, ref, old_doc[k][ind]))
                        if update_value:
                            print('Updating with new value: {}'.format(v[i]))
                            old_doc[k][ind] = v[i]
                        else:
                            print('Skipping insert for {} from {}'.format(k, ref))

                        continue  # skips appending the entry for this particular reference

                    old_doc[k] = np.append(old_doc[k], [v[i]])

        # Replace document in the database
        if self.use_mongodb:
            self.load_to_mongodb(old_doc)
        else:
            orig_doc = self.query_db({id_column: name})[0]
            ind = np.where(self.db == orig_doc)
            self.db[ind] = old_doc

        if not auto_save:
            print('Data for {} has been updated. Consider running save_all() to update JSON on disk.'.format(name))
        if auto_save:
            print('Auto-saving to {}'.format(save_dir))
            self.save_from_db(old_doc, out_dir=save_dir)

    def query_db(self, query, embed_ref=False, ref_id_column='key'):
        """
        Perform a database query with MongoDB's query language.
        Examples:
            db.query({'name': 'And XXX'})
            db.query({'ra.value': 10.68458})
        Query examples:
            Greather than case: query = {'surface_brightness.value': {'$gt': 27}}
            EXISTS case: query = {'stellar_radial_velocity_dispersion.value': {'$exists': True}}
            AND case: query = {'surface_brightness.value': {'$gt': 27}, 'radial_velocity.value': {'$lte': -100}}
            OR case: query = {'$or': [{'ra.value': 10.68458}, {'dec.value': 49.64667}]}
            AND and OR case: query = {'surface_brightness.value': {'$gt': 27}, '$or': [
                {'surface_brightness.error_upper': {'$lte': 0.5}},
                {'surface_brightness.error_lower': {'$gte': 1.0}}]}

        Parameters
        ----------
        query : dict
            Query to perform. Uses MongoDB's query language.
        embed_ref : bool
            Flag whether or not references should be embedded in the output document (Default: False)
        ref_id_column : str
            Field name to use when matching references (Default: 'key')

        Returns
        -------
        result : np.array
            Numpy array of document results
        """

        if self.use_mongodb:
            result = self._query_mongodb(query)
        else:
            result = self._query_manual(query)

        # Embed the reference dict in place of the key
        if embed_ref:
            result = deepcopy(result)
            for doc in result:
                for key, value in doc.items():
                    if not isinstance(value, (list, np.ndarray)):
                        continue
                    for i, each_val in enumerate(value):
                        ref_key = each_val.get('reference')
                        if ref_key:
                            ref = self.query_reference({ref_id_column: ref_key})
                            if isinstance(ref, (list, np.ndarray)) and len(ref) > 0:
                                doc[key][i]['reference'] = ref[0]

        return result

    def query(self, *args, **kwargs):
        return self.query_db(*args, **kwargs)

    def query_reference(self, query):
        """
        Query references. Examples:
            db.query_reference({'id': 1})
            db.query_reference({'key': 'Bellazzini_2006_1'})[0]

        Parameters
        ----------
        query : dict
            Query to use. Uses MongoDB's query language.

        Returns
        -------
        result : np.array
            Numpy array of document results
        """

        if self.use_mongodb:
            result = list(self.references.find(query))
            for r in result:
                # Remove the internal MongoDB IDs
                del r['_id']
        else:
            result = self.references
            for key, value in query.items():
                # Use things like dec.value to query [value] for each element in [dec]
                key_list = key.split('.')
                if not key_list[0].startswith('$'):
                    result = self._sub_query(result, key_list[0])
                    if len(result) >= 1:
                        result = self._sub_query(result, key_list, value)
                else:
                    result = self._sub_query(result, key_list, value)

        return result

    def _query_mongodb(self, query):
        # Send query to MondoDB

        cursor = self.db.find(query)
        out_result = np.array([self._recursive_json_fix(d) for d in cursor])

        return out_result

    def _query_manual(self, query):
        # Manually execute query to in-memory database
        out_result = self.db
        for key, value in query.items():
            # Use strings like dec.value to query [value] for each element in [dec]
            key_list = key.split('.')
            if not key_list[0].startswith('$'):
                out_result = self._sub_query(out_result, key_list[0])
                if len(out_result) >= 1:
                    # important to be >= 1 otherwise it just behaves like an $exist
                    out_result = self._sub_query(out_result, key_list, value)
            else:
                out_result = self._sub_query(out_result, key_list, value)

        return out_result

    def _sub_query(self, doc_list, key, value=None):
        # Method to perform query, called by _query_manual

        special_operator = False
        if isinstance(key, list) and key[0].startswith('$'):
            # Handle cases that start with $ like $or statements
            special_operator = True

        if value is None:
            # Basically check if this key exists
            out_result = np.array(list(filter(lambda new_doc: key in new_doc, doc_list)))
        else:
            if (isinstance(key, list) and len(key) > 1) or special_operator:
                ind_list = []
                for i, elem in enumerate(doc_list):
                    if isinstance(value, dict) or isinstance(value, list):
                        # Special operator
                        if isinstance(value, list):
                            # Handle cases with $or
                            db_operator = key[0]
                            sub_value = value
                        else:
                            db_operator, sub_value = list(value.items())[0]
                        if db_operator == '$gt':
                            temp_list = list(filter(lambda y: y.get(key[1]) > sub_value, elem[key[0]]))
                        elif db_operator == '$gte':
                            temp_list = list(filter(lambda y: y.get(key[1]) >= sub_value, elem[key[0]]))
                        elif db_operator == '$lt':
                            temp_list = list(filter(lambda y: y.get(key[1]) < sub_value, elem[key[0]]))
                        elif db_operator == '$lte':
                            temp_list = list(filter(lambda y: y.get(key[1]) <= sub_value, elem[key[0]]))
                        elif db_operator == '$exists':
                            raise RuntimeError('Use of $exists has been rolled back and '
                                               'only works with the MongoDB implementation.')
                            # if sub_value:
                            #     temp_list = list(filter(lambda y: key[1] in y, elem[key[0]]))
                            # else:
                            #     temp_list = list(filter(lambda y: key[1] not in y, elem[key[0]]))
                        elif db_operator == '$or':
                            # Special logic for $or (run each sub_query in the or list against the current element)
                            temp_list = []
                            for sub_sub_query in sub_value:
                                for k, v in sub_sub_query.items():
                                    # Use things like dec.value to query [value] for each element in [dec]
                                    key_list = k.split('.')
                                    temp = self._sub_query(np.array([elem]), key_list[0])
                                    if len(temp) > 0:
                                        temp = self._sub_query(temp, key_list, v)
                                    temp_list += list(temp)
                        else:
                            raise RuntimeError('ERROR: {} not yet supported'.format(db_operator))
                    else:
                        temp_list = list(filter(lambda y: y.get(key[1]) == value, elem[key[0]]))
                    if len(temp_list) > 0:
                        ind_list.append(i)
                out_result = doc_list[ind_list]
            else:
                # Straight-forward check, for things that are not embedded (eg, name)
                out_result = np.array(list(filter(lambda doc: doc[key[0]] == value, doc_list)))

        return out_result

    @staticmethod
    def _store_quantity(val, unit):
        # Method to convert a value to a Quantity, if a unit is provided
        if unit:
            try:
                val = Quantity(val, unit=unit)
            except ValueError:
                # Ignore unrecognized units
                pass

        return val

    def generate_curation(self, reference, existing_curation={}):
        """
        Generate a curation dictionary for use in query_table or that can be exported.
        This will loop over all parameters saving only those parameters that contain the reference.

        Parameters
        ----------
        reference : str or list
            Reference to use in the curation. If a list, this will recursive build the curation assuming a sorted order
        existing_curation : dict
            Existing curation to append new values to (appended values will overwrite existing ones)

        Returns
        -------
        curation : dict
        """

        curation = existing_curation.copy()

        if isinstance(reference, (list, np.ndarray)):
            reverse_list = reference[::-1]
            for ref in reverse_list:
                curation = self.generate_curation(ref, existing_curation=curation)
        else:
            for doc in self.query_db(query={}):
                for k, v_list in doc.items():
                    if not isinstance(v_list, (list, np.ndarray)):
                        continue
                    for v in v_list:
                        if v.get('reference') == reference:
                            curation[k] = reference

        return curation

    def query_table(self, query={}, curation={}, selection={}, reorder_columns_rowidx=0,
                          add_coordinates=True, use_qtable=True):
        """
        Get a formatted table of all query results. When multiple results are present for a single value, the best one
        is picked unless the user specifies a selection. This functionality will be revisited in the future.
        The output is as a QTable which allows Quantities to be embedded in the results.

        Parameters
        ----------
        query : dict
            Query to use in MongoDB query language. Default is an empty dictionary for all results.
        curation : dict or str
            Name of file to use as curation for the data or dictionary with the field value and reference to use for
            it (otherwise will pick best=1)
        selection : dict
            Dictionary of overwrites for the supplied curation
        reorder_columns_rowidx : int or None
            Row/entry index to use as a template for column order.  If None, use
            an undefined order (determined by `astropy.Table` initializer).
        add_coordinates : bool or str
            If True, adds a 'coord' column to the table (if it does not
            exist) with a SkyCoord for this table.  If 'raise', raises an
            exception if this fails, otherwise a warning is generated.
        use_qtable : bool
            If True, the result is a QTable, otherwise, a Table

        Returns
        -------
        df : astropy.table.QTable or astropy.table.Table
            Astropy QTable of results
        """

        results = self.query_db(query=query)

        # Load curation file (JSON of best values to use)
        curation_dict = _read_curation(curation)

        # If user as provided any selection values, overwrite the curation settings
        if selection:
            curation_dict.update(selection)

        # For each entry in result, select best field.value or what the user has specified
        tab_data = []
        for entry in results:
            out_row = {}
            for key, val in entry.items():
                if not isinstance(val, (list, type(np.array([])))):
                    out_row[key] = val
                else:
                    # Select best one to return
                    if len(val) > 1:
                        if key in curation_dict.keys():
                            # If selection listed a key, use the reference information there
                            ind = np.array([x['reference'] for x in val]) == curation_dict[key]
                        else:
                            ind = np.array([x.get('best', 0) for x in val]) == 1

                        # Only proceed if you have any results to consider
                        if len(val[ind]) > 0:
                            unit = val[ind][0].get('unit')
                            if val[ind][0].get('distribution') is not None:
                                temp_dic = _get_values_from_distribution(val[ind][0].get('distribution'))
                                temp_val = temp_dic['value']
                            else:
                                temp_val = val[ind][0]['value']
                            out_row[key] = self._store_quantity(temp_val, unit)
                    else:
                        unit = val[0].get('unit')
                        if val[0].get('distribution') is not None:
                            temp_dic = _get_values_from_distribution(val[0].get('distribution'))
                            temp_val = temp_dic['value']
                        else:
                            temp_val = val[0]['value']
                        out_row[key] = self._store_quantity(temp_val, unit)

            tab_data.append(out_row)

        if use_qtable:
            tab = QTable(tab_data)
        else:
            tab = Table(tab_data)

        if add_coordinates:
            if 'coord' in tab.colnames:
                warnings.warn('"coord" column already in database table, '
                              'not adding coordinates automatically')
            else:
                try:
                    coo = SkyCoord.guess_from_table(tab)
                except Exception as e:
                    if add_coordinates == 'raise':
                        raise e
                    else:
                        warnings.warn(f'Failed to add coordinates - exception '
                                       'raised in guess_from_table: {e}')
                else:
                    tab['coord'] = coo

        if reorder_columns_rowidx is None and len(tab_data) > 0:
            return tab
        elif len(tab_data) == 0:
            return tab
        else:
            reorder_row_colnames = list(tab_data[reorder_columns_rowidx].keys())
            for colname in tab.colnames:
                if colname not in reorder_row_colnames:
                    reorder_row_colnames.append(colname)
            return tab[reorder_row_colnames]

    def table(self, *args, **kwargs):
        return self.query_table(*args, **kwargs)
