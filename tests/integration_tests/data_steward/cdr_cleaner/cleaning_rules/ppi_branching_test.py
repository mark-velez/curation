import datetime
import time
from typing import Any, Optional

from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, Table, TableReference

import app_identity
import bq_utils
import resources
import sandbox
from cdr_cleaner.cleaning_rules.ppi_branching import PpiBranching, OBSERVATION_BACKUP_TABLE_ID
from data_steward.cdr_cleaner.cleaning_rules import ppi_branching
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from utils import bq

TEST_DATA_FIELDS = (
 'observation_id', 'person_id', 'observation_source_value', 'value_source_value', 'value_as_string'
)
"""The columns associated with `TEST_DATA_ROWS`"""

TEST_DATA_ROWS = {
    (2000, 2000, 'Race_WhatRaceEthnicity', 'WhatRaceEthnicity_RaceEthnicityNoneOfThese', None),
    (2001, 2000, 'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox', None, 'Mexican and Filipino'),
    (3000, 3000, 'Race_WhatRaceEthnicity', 'WhatRaceEthnicity_White', None),
    (3001, 3000, 'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox', 'PMI_Skip', None),
    (4000, 4000, 'OverallHealth_OrganTransplant', 'OrganTransplant_Yes', None),
    (4001, 4000, 'OrganTransplant_OrganTransplantDescription', None, 'Cornea'),
    (5000, 5000, 'OverallHealth_OrganTransplant', 'OrganTransplant_No', None),
    (5001, 5000, 'OrganTransplant_OrganTransplantDescription', 'PMI_Skip', None)}
"""Set of tuples used to create rows in the observation table"""

TEST_DATA_DROP = {r for r in TEST_DATA_ROWS if r[0] in (3001, 5001)}
"""Set of tuples in TEST_DATA_ROWS that should be removed after rule is run"""

TEST_DATA_KEEP = set(TEST_DATA_ROWS) - set(TEST_DATA_DROP)
"""Set of tuples in TEST_DATA_ROWS that should remain after rule is run"""


def _default_value_for(field: bigquery.SchemaField) -> Optional[Any]:
    """
    Get a default dummy value for a field. Used to create test observation rows more easily.

    :param field: the field
    :return: a value
    """
    if field.name.endswith('concept_id'):
        return 0
    if field.mode == 'required':
        if field.field_type == 'integer':
            return 0
        elif field.field_type == 'date':
            return datetime.datetime.today().strftime('%Y-%m-%d')
        elif field.field_type == 'timestamp':
            return time.time()
    return None


class Observation(object):
    """
    Helper class to initialize test observation rows
    """

    SCHEMA = bq.get_table_schema('observation')
    """List of schema fields for observation table"""

    _FIELD_DEFAULTS = dict(
        (field.name, _default_value_for(field)) for field in SCHEMA)
    """Maps field names to default values"""

    def __init__(self, **kwargs):
        # only permit observation fields as args
        for prop, val in kwargs.items():
            if prop not in Observation._FIELD_DEFAULTS.keys():
                raise ValueError(f'Supplied key {prop} is not a field in the observation table')
            self.__setattr__(prop, val)
        # unset args are set to a (dummy) default value
        for field_name, default_val in Observation._FIELD_DEFAULTS.items():
            if field_name not in kwargs.keys():
                self.__setattr__(field_name, default_val)


def _fq_table_name(table: Table):
    return f'{table.project}.{table.dataset_id}.{table.table_id}'


class PPiBranchingTest(BaseTest.CleaningRulesTestBase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()
        project_id = app_identity.get_application_id()
        dataset_id = bq_utils.get_rdr_dataset_id()
        sandbox_dataset_id = sandbox.get_sandbox_dataset_id(dataset_id)
        rule = PpiBranching(project_id, dataset_id, sandbox_dataset_id)
        cls.dataset_id = dataset_id
        cls.project_id = project_id
        cls.query_class = rule
        cls.fq_sandbox_table_names = [_fq_table_name(table)
                                      for table in (rule.lookup_table, rule.backup_table)]
        cls.fq_table_names = [_fq_table_name(rule.observation_table)]
        super().setUpClass()

    def setUp(self):
        self.data = [Observation(**dict(zip(TEST_DATA_FIELDS, row))).__dict__
                     for row in TEST_DATA_ROWS]
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.schema = Observation.SCHEMA
        self.client.load_table_from_json(self.data,
                                         destination=f'{self.dataset_id}.{ppi_branching.OBSERVATION}',
                                         job_config=job_config).result()

    def load_observation_table(self):
        """
        Drop existing and create observation table loaded with test data
        :return:
        """
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.schema = Observation.SCHEMA
        self.client.load_table_from_json(self.data,
                                         destination=f'{self.dataset_id}.{ppi_branching.OBSERVATION}',
                                         job_config=job_config).result()

    def _query(self, q: str) -> bigquery.table.RowIterator:
        """
        Execute query and return results

        :param q: the query
        :return: results as an iterable collection of Row objects
        """
        query_job = self.client.query(q)
        return query_job.result()

    def assertJobSuccess(self, job):
        self.assertEqual(job.state, 'DONE')
        self.assertIsNone(job.error_result)
        self.assertIsNone(job.errors)

    def test(self):
        loaded_ids = [_id for (_id, *_) in TEST_DATA_ROWS]
        sandboxed_ids = [_id for (_id, *_) in TEST_DATA_DROP]
        cleaned_values = [(_id,) for (_id, *_) in TEST_DATA_KEEP]
        tables_and_counts = [{
            'name': self.query_class.observation_table.table_id,
            'fq_table_name': _fq_table_name(self.query_class.observation_table),
            'fq_sandbox_table_name': _fq_table_name(self.query_class.backup_table),
            'fields': ['observation_id'],
            'loaded_ids': loaded_ids,
            'sandboxed_ids': sandboxed_ids,
            'cleaned_values': cleaned_values
        }]

        self.default_test(tables_and_counts)

    def test_rule(self):
        rules_df = ppi_branching._load_dataframe(resources.PPI_BRANCHING_RULE_PATHS)

        # create lookup table
        lookup_job = ppi_branching.load_rules_lookup(client=self.client,
                                                     destination_table=self.lookup_table,
                                                     rule_paths=resources.PPI_BRANCHING_RULE_PATHS)
        self.assertIsInstance(lookup_job, bigquery.LoadJob)
        self.assertJobSuccess(lookup_job)
        q = f'SELECT * FROM {self.lookup_table.dataset_id}.{self.lookup_table.table_id}'
        row_iter = self._query(q)
        self.assertEqual(row_iter.total_rows, len(rules_df.index))
        # existing lookup gets overwritten
        lookup_job = ppi_branching.load_rules_lookup(client=self.client,
                                                     destination_table=self.lookup_table,
                                                     rule_paths=resources.PPI_BRANCHING_RULE_PATHS)
        self.assertJobSuccess(lookup_job)
        row_iter = self._query(q)
        self.assertEqual(row_iter.total_rows, len(rules_df.index))

        # subsequent tests rely on observation test data
        self.load_observation_table()

        # backup rows
        backup_job = ppi_branching.backup_rows(src_table=self.observation_table,
                                               dst_table=self.backup_table,
                                               lookup_table=self.lookup_table,
                                               client=self.client)
        self.assertIsInstance(backup_job, bigquery.QueryJob)
        self.assertJobSuccess(backup_job)
        q = f'''SELECT * FROM {self.backup_table.dataset_id}.{self.backup_table.table_id} 
                ORDER BY observation_id'''
        actual_result = {tuple(row[f] for f in TEST_DATA_FIELDS) for row in self._query(q)}
        self.assertSetEqual(TEST_DATA_DROP, actual_result)
        # existing backup gets overwritten
        backup_job = ppi_branching.backup_rows(src_table=self.observation_table,
                                               dst_table=self.backup_table,
                                               lookup_table=self.lookup_table,
                                               client=self.client)
        self.assertJobSuccess(backup_job)
        actual_result = {tuple(row[f] for f in TEST_DATA_FIELDS) for row in self._query(q)}
        self.assertSetEqual(TEST_DATA_DROP, actual_result)

        # drop rows
        drop_job = ppi_branching.drop_rows(self.client,
                                           src_table=self.observation_table,
                                           backup_table=self.backup_table)
        self.assertIsInstance(drop_job, bigquery.QueryJob)
        self.assertJobSuccess(drop_job)
        q = f'''SELECT * FROM {self.observation_table.dataset_id}.{self.observation_table.table_id} 
                ORDER BY observation_id'''
        actual_result = {tuple(row[f] for f in TEST_DATA_FIELDS) for row in self._query(q)}
        self.assertSetEqual(actual_result, TEST_DATA_KEEP)
        # repeated drop job has no effect
        drop_job = ppi_branching.drop_rows(self.client,
                                           src_table=self.observation_table,
                                           backup_table=self.backup_table)
        self.assertJobSuccess(drop_job)
        actual_result = {tuple(row[f] for f in TEST_DATA_FIELDS) for row in self._query(q)}
        self.assertSetEqual(actual_result, TEST_DATA_KEEP)
