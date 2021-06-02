# Python imports
import json
import unittest
import os
import random
import string

# Third party imports
from google.cloud import bigquery
import pandas as pd

# Project Imports
from utils import bq
import app_identity
from constants.utils import bq as consts


class BQTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    @staticmethod
    def get_random_dataset_id():
        suffix = ''.join(random.choice(string.ascii_letters) for i in range(16))
        return f'{__name__}_{suffix}'

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = self.get_random_dataset_id()
        self.description = f'Dataset for {__name__} integration tests'
        self.label_or_tag = {'test': 'bq'}
        self.client = bq.get_client(self.project_id)
        # Remove dataset if it already exists
        self.client.delete_dataset(self.dataset_id,
                                   delete_contents=True,
                                   not_found_ok=True)

    def test_create_dataset(self):
        dataset = bq.create_dataset(self.project_id, self.dataset_id,
                                    self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

        # Try to create same dataset, which now already exists
        self.assertRaises(RuntimeError, bq.create_dataset, self.project_id,
                          self.dataset_id, self.description, self.label_or_tag)

        dataset = bq.create_dataset(self.project_id,
                                    self.dataset_id,
                                    self.description,
                                    self.label_or_tag,
                                    overwrite_existing=True)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def test_define_dataset(self):
        self.assertRaises(RuntimeError, bq.define_dataset, None,
                          self.dataset_id, self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, '', self.dataset_id,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          False, self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          self.dataset_id, ' ', self.label_or_tag)
        self.assertRaises(RuntimeError, bq.define_dataset, self.project_id,
                          self.dataset_id, self.description, None)
        dataset = bq.define_dataset(self.project_id, self.dataset_id,
                                    self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def test_query_sheet_linked_bq_table(self):
        dataset = bq.create_dataset(self.project_id, self.dataset_id,
                                    self.description, self.label_or_tag)
        # add Google Drive scope
        external_data_scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
        client = bq.get_client(self.project_id, external_data_scopes)

        # Configure the external data source and query job.
        external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")

        # Grant viewing access to the test sheet to BigQuery test accounts
        sheet_url = (
            "https://docs.google.com/spreadsheets"
            "/d/1JI-KyigmwZU9I2J6TZqVTPNoEAWVqiFeF8Y549-dvzM/edit#gid=0")
        schema = [
            bigquery.SchemaField("site_name", "STRING"),
            bigquery.SchemaField("hpo_id", "STRING"),
            bigquery.SchemaField("site_point_of_contact", "STRING"),
        ]
        external_config.source_uris = [sheet_url]
        external_config.schema = schema
        external_config.options.range = (
            "contacts!A1:C5"  # limit scope so that other items can be added to sheet
        )
        external_config.options.skip_leading_rows = 1  # Optionally skip header row.

        table_id = consts.HPO_ID_CONTACT_LIST_TABLE_ID
        table = bigquery.Table(dataset.table(table_id), schema=schema)
        table.external_data_configuration = external_config

        table = client.create_table(table)
        table_content_query = f'SELECT * FROM `{dataset.dataset_id}.{table.table_id}`'
        actual_df = bq.query_sheet_linked_bq_table(self.project_id,
                                                   table_content_query,
                                                   external_data_scopes)
        expected_dict = [{
            'site_name':
                'Fake Site Name 1',
            'hpo_id':
                'fake_1',
            'site_point_of_contact':
                'fake.email.1@site_1.fakedomain; fake.email.2@site_1.fakedomain'
        }, {
            'site_name': 'Fake Site Name 2',
            'hpo_id': 'fake_2',
            'site_point_of_contact': 'no data steward'
        }, {
            'site_name':
                'Fake Site Name 3',
            'hpo_id':
                'fake_3',
            'site_point_of_contact':
                'Fake.Email.1@site_3.fake_domain; Fake.Email.2@site_3.fake_domain'
        }, {
            'site_name':
                'Fake Site Name 4',
            'hpo_id':
                'fake_4',
            'site_point_of_contact':
                'FAKE.EMAIL.1@site4.fakedomain; FAKE.EMAIL.2@site4.fakedomain'
        }]
        expected_df = pd.DataFrame(
            expected_dict,
            columns=["site_name", "hpo_id", "site_point_of_contact"])
        pd.testing.assert_frame_equal(actual_df, expected_df)

    @staticmethod
    def get_current_gcp_account():
        """
        Get the account (client_email) associated with the key file
        :return: the email of the account
        """
        creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        with open(creds_path) as creds_fp:
            creds = json.load(creds_fp)
        return creds.get('client_email')

    def test_update_dataset_access_entries(self):
        dataset = self.client.create_dataset(self.dataset_id)
        expected = dataset.access_entries.copy()
        # add an access entry that would not be present by default
        # valid account needed for test to pass
        entity_id = self.get_current_gcp_account()
        entry = bigquery.AccessEntry(role='READER',
                                     entity_type="userByEmail",
                                     entity_id=entity_id)
        expected.append(entry)
        bq.update_dataset_access_entries(self.client, dataset, expected)
        dataset = self.client.get_dataset(self.dataset_id)
        self.assertSequenceEqual(expected, dataset.access_entries)

    def tearDown(self):
        self.client.delete_dataset(self.dataset_id,
                                   delete_contents=True,
                                   not_found_ok=True)
