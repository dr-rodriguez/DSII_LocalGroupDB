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

    def save_from_db(self):
        # Save a JSON representation to disk
        pass

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

        return self.db

    def table(self, query={}, selection={}):
        # Get nicely-formatted table of all results (or of the query)
        # selection is a dictionary with the field value and the reference to use for it (otherwise will pick best=1)
        results = self.query(query=query)

        # For each entry in result, select best field.value or what the user has specified
        tab_data = []
        for entry in results:
            out_row = {}
            for key, val in entry.items():
                if key in selection.keys():
                    # TODO: select specified one
                    pass
                elif not isinstance(val, (list, type(np.array([])))):
                    out_row[key] = val
                else:
                    # Select best one to return
                    if len(val) > 1:
                        ind = np.array([x['best'] for x in val]) == 1
                        out_row[key] = val[ind][0]['value']
                    else:
                        out_row[key] = val[0]['value']
            tab_data.append(out_row)

        # Convert to pandas
        df = pd.DataFrame(tab_data)

        return df
