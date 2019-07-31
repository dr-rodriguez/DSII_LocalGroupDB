# Core functionality for database implementation
import os
import json
import numpy as np
import pandas as pd


class Database(object):
    def __init__(self, directory='data', conn_string=''):
        # Load or establish connection

        self.use_mongodb = False

        if conn_string:
            # Connect to mongoDB
            pass
            # self.use_mongodb = True
        else:
            self.db = np.array([])
            self.load_all(directory)

    def load_all(self, directory):
        for filename in os.listdir(directory):
            # Skip hidden and non-json files
            if filename.startswith('.') or not filename.endswith('.json'):
                continue

            self.load_to_db(os.path.join(directory, filename))

    def load_to_db(self, filename):
        # Load JSON file to database
        with open(filename, 'r') as f:
            doc = json.load(f)

        doc = self._recursive_json_fix(doc)

        self.db = np.append(self.db, doc)

    def _recursive_json_fix(self, doc):
        out_doc = {}
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
        return out_doc

    def _recursive_json_reverse_fix(self, doc):
        out_doc = {}
        for key, val in doc.items():
            if isinstance(val, dict):
                out_doc[key] = self._recursive_json_fix(val)
            elif isinstance(val, type(np.array([]))):
                new_array = []
                for elem in val:
                    new_val = self._recursive_json_fix(elem)
                    new_array.append(new_val)
                out_doc[key] = new_array
            else:
                out_doc[key] = val
        return out_doc

    def save_from_db(self, doc, verbose=False, out_dir='', save=False, name=''):
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
            with open(os.path.join(out_dir, filename), 'w') as f:
                f.write(out_json)
        return

    def query(self, query):
        if self.use_mongodb:
            result = self._query_mongodb(query)
        else:
            result = self._query_manual(query)

        return result

    def _query_mongodb(self, query):
        # Send query to MondoDB
        return None

    def _query_manual(self, query):
        # Manually execute query to in-memory database
        out_result = self.db
        for key, value in query.items():
            # Use things like dec.value to query [value] for each element in [dec]
            key_list = key.split('.')
            if not key_list[0].startswith('$'):
                out_result = self._sub_query(out_result, key_list[0])
                if len(out_result) > 1:
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
                            temp_list = list(filter(lambda y: y[key[1]] > sub_value, elem[key[0]]))
                        elif db_operator == '$gte':
                            temp_list = list(filter(lambda y: y[key[1]] >= sub_value, elem[key[0]]))
                        elif db_operator == '$lt':
                            temp_list = list(filter(lambda y: y[key[1]] < sub_value, elem[key[0]]))
                        elif db_operator == '$lte':
                            temp_list = list(filter(lambda y: y[key[1]] <= sub_value, elem[key[0]]))
                        elif db_operator == '$exists':
                            if sub_value:
                                temp_list = list(filter(lambda y: key[1] in y, elem[key[0]]))
                            else:
                                temp_list = list(filter(lambda y: key[1] not in y, elem[key[0]]))
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
                        temp_list = list(filter(lambda y: y[key[1]] == value, elem[key[0]]))
                    if len(temp_list) > 0:
                        ind_list.append(i)
                out_result = doc_list[ind_list]
            else:
                # Straight-forward check, for things that are not embedded (eg, name)
                out_result = np.array(list(filter(lambda doc: doc[key[0]] == value, doc_list)))

        return out_result

    def table(self, query={}, selection={}):
        # Get nicely-formatted table of all results (or of the query)
        # selection is a dictionary with the field value and the reference to use for it (otherwise will pick best=1)
        results = self.query(query=query)

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
                        if key in selection.keys():
                            # If selection listed a key, use the reference information there
                            ind = np.array([x['reference'] for x in val]) == selection[key]
                        else:
                            ind = np.array([x['best'] for x in val]) == 1
                        out_row[key] = val[ind][0]['value']
                    else:
                        out_row[key] = val[0]['value']
            tab_data.append(out_row)

        # Convert to pandas
        df = pd.DataFrame(tab_data)

        return df
