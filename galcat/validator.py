# Validate JSON

import json
from astropy.units import Quantity


class Validator(object):
    def __init__(self, database, db_object=None, is_data=True, id_column='name', ref_check=True, verbose=False):
        """
        Validator object to check that JSON passes all criteria for ingesting into main database.

        Parameters
        ----------
        database : galcat.core.Database
            Database object, used to validate values and references
        db_object : None, str, dict-like
            Object to validate. If None (default), will work against full database.
            If string, will treat as JSON file input.
        is_data : bool
            Flag to indicate this is data contents as opposed to reference content (Default: True)
        id_column : str
            Field to use when matching names (Default: 'name')
        ref_check : bool
            Flag to indicate if references should be validated
        verbose : bool
            Flag to control verbosity
        """

        self.run_full_db = False

        if db_object is None:
            self.run_full_db = True
        elif db_object is not None and isinstance(db_object, str):
            with open(db_object, 'r') as f:
                self.doc = json.load(f)
        elif db_object is not None:
            self.doc = db_object

        self.id_column = id_column
        self.is_data = is_data
        self.db = database
        self.ref_check = ref_check
        self.verbose = verbose

    def run(self):
        # Run checks
        if self.run_full_db:
            result_list = []
            for doc in self.db.query_db({}):
                self.doc = doc
                result = self.run_one()
                result_list.append(result)
            result = all(result_list)
        else:
            result = self.run_one()
        print('Validation complete.')
        return result

    def run_one(self):
        if self.is_data:
            name = self.check_name()
            if self.verbose: print('Checking {}'.format(name))
            exist_check = self.check_exists(name)
            val_check = self.check_values()

            if all([exist_check, val_check]):
                return True
            else:
                return False
        else:
            date_check = self.check_dates()

    def check_name(self):
        """Checks that a name has been provided for the JSON"""
        name = self.doc.get(self.id_column)
        if name:
            return name
        else:
            raise RuntimeError('JSON does not provide a valid name in the {} field.'.format(self.id_column))

    def check_exists(self, name):
        """Checks that the provided name exists in the database."""
        doc_list = self.db.query_db({self.id_column: name})
        if len(doc_list) > 0:
            return True
        else:
            print('WARNING: This JSON represents a new/unmatched object: {}. '
                  'Use load_file_to_db() to load new objects.'.format(name))
            return False

    def check_values(self):
        """Check that all fields in document contain value or distribution fields.
        Also checks references for each field.
        """
        result = [True]

        for k, v in self.doc.items():
            # Skip the name field
            if k == self.id_column:
                continue

            # Loop over all value entries
            for elem in v:
                if elem.get('value') is None and elem.get('distribution') is None:
                    print('ERROR: {} has missing values/distribution: {}'.format(k, elem))
                    result.append(False)
                if self.ref_check and not self.check_references(elem):
                    print('ERROR: {} has missing references or it does not exist: {}'.format(k, elem))
                    result.append(False)
                if elem.get('unit'):
                    # If unit is present, check that it's valid
                    unit_check = self.check_unit(elem)
                    if not unit_check:
                        print('ERROR: {} has invalid units: {}'.format(k, elem.get('unit')))
                        result.append(False)

        return all(result)

    def check_references(self, elem, id_column='key'):
        """Check that references are provided and that they already exist in the database"""
        ref = elem.get('reference')
        if ref is None or ref == '':
            return False
        else:
            db_ref = self.db.query_reference({id_column: ref})
            if self.ref_check:
                if len(db_ref) == 0:
                    return False
                if db_ref[0].get(id_column) != ref:
                    return False
            return True

    @staticmethod
    def check_unit(elem):
        # Check that unit is recognized by astropy.units (or is empty)
        if elem.get('unit') == '':
            return True

        try:
            _ = Quantity(1, unit=elem.get('unit'))
            return True
        except ValueError:
            return False

    def check_dates(self):
        return True
