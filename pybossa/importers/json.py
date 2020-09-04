# -*- coding: utf8 -*-
# This file is part of PYBOSSA.
#
# Copyright (C) 2015 Scifabric LTD.
#
# PYBOSSA is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PYBOSSA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with PYBOSSA.  If not, see <http://www.gnu.org/licenses/>.

from flask_babel import gettext
import pandas as pd

from .base import BulkTaskImport, BulkImportException

class BulkTaskJSONImport(BulkTaskImport):

    """Class to import JSON tasks in bulk."""

    importer_id = "json"

    def __init__(self, json_data, last_import_meta=None):
        self.data = json_data
        self.last_import_meta = last_import_meta

    def tasks(self):
        """Get tasks from a given URL."""
        json_df = pd.from_dict(self._get_json_data())
        return self._import_json_tasks(json_df)

    def count_tasks(self):
        return len([task for task in self.tasks()])
        
    def _get_json_data(self):
        """Get data from JSON."""
        return self.data

    def _import_json_tasks(self, json_df):
        """Import JSON tasks."""
        headers = []
        fields = set(['state', 'quorum', 'calibration', 'priority_0',
                      'n_answers'])
        field_header_index = []
        row_number = 0

        headers = list(json_df.columns)
        self._check_no_duplicated_headers(headers)
        self._check_no_empty_headers(headers)
        field_headers = set(headers) & fields
        for field in field_headers:
            field_header_index.append(headers.index(field))

        self._check_valid_row_length(json_df)

        for index, row in json_df.iterrows():
            row_number += 1
            task_data = {"info": {}}
            for idx, cell in enumerate(list(row)):
                if idx in field_header_index:
                    task_data[headers[idx]] = cell
                else:
                    task_data["info"][headers[idx]] = cell
            yield task_data

    def _check_no_duplicated_headers(self, headers):
        if len(headers) != len(set(headers)):
            msg = gettext('The JSON you provided has '
                          'two headers with the same name.')
            raise BulkImportException(msg)

    def _check_no_empty_headers(self, headers):
        stripped_headers = [header.strip() for header in headers]
        for h in stripped_headers:
            if "Unnamed" in h:
                position = stripped_headers.index(h)
                msg = "The JSON you provided is malformed {}.".format(position+1)
                raise BulkImportException(msg)

    def _check_valid_row_length(self, df):
        if type(df.index) is not pd.RangeIndex:
            msg = "The JSON you provided is malformed."
            raise BulkImportException(msg)
