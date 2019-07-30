# Core functionality for database implementation
import os
import json
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
            self.db = []
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
        self.db.append(doc)

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
        return self.db

    def table(self, query={}, selection={}):
        # Get nicely-formatted table of all results (or of the query)
        # selection is a dictionary with the field value and the reference to use for it (otherwise will pick best=1)
        results = self.query(query=query)

        # For each entry in result, select best field.value or what the user has specified
        tab_data = []
        for entry in results:
            out_row = {}
            for key, value in entry.items():
                if key in selection.keys():
                    # TODO: select specified one
                    pass
                elif not isinstance(value, list):
                    out_row[key] = value
                else:
                    # TODO: select best one
                    if len(value) > 1:
                        # Select best one
                        pass
                    else:
                        out_row[key] = value[0]['value']
            tab_data.append(out_row)

        # Convert to pandas
        df = pd.DataFrame(tab_data)

        return df
