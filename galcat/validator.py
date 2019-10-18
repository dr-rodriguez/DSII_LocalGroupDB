# Validate JSON

import json


class Validator(object):
    def __init__(self, filename, database, is_data=True, id_column='name'):
        """
        Validator object to check that JSON passes all criteria for ingesting into main database.

        Parameters
        ----------
        filename : str
            JSON file to load into the database
        database : galcat.core.Database
            Database object, used to validate values and references
        is_data : bool
            Flag to indicate this is data contents as opposed to reference content (Default: True)
        id_column : str
            Field to use when matching names (Default: 'name')
        """

        with open(filename, 'r') as f:
            self.doc = json.load(f)

        self.id_column = id_column
        self.is_data = is_data
        self.db = database

        # Run checks
        if is_data:
            name = self.check_name()
            exist_check = self.check_exists(name)
            val_check = self.check_values()

    def check_name(self):
        """Checks that a name has been provided for the JSON"""
        name = self.doc.get(self.id_column)
        if name:
            return name
        else:
            raise RuntimeError('JSON does not provide a valid name in the {} field.'.format(self.id_column))

    def check_exists(self, name):
        """Checks that the provided name exists in the database."""
        doc_list = self.db.query({self.id_column: name})
        if len(doc_list) > 0:
            return True
        else:
            print('WARNING: This JSON represents a new/unmatched object: {}'.format(name))
            return False

    def check_values(self):
        """Check that all fields in document contain value or distribution fields.
        Also checks references for each field.
        """
        for k, v in self.doc.items():
            # Skip the name field
            if k == self.id_column:
                continue

            # Loop over all value entries
            for elem in v:
                if elem.get('value') is None or elem.get('distribution') is None:
                    print('ERROR: {} has missing values/distribution: {}'.format(k, elem))
                    return False
                if not self.check_references(elem):
                    msg = 'ERROR: {} has missing references: {}'.format(k, elem)
                    print(msg)
                    return False

        return True

    def check_references(self, elem):
        ref = elem.get('reference')
        if ref is None or ref == '':
            return False
        else:
            return True

    def check_dates(self):
        pass
