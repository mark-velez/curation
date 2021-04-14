# for data manipulation
import pandas as pd 

# Path
from code.config import (CSV_FOLDER, CHECK_LIST_CSV_FILE)

# SQL template
from jinja2 import Template

# functions for QC
from code.check_table_suppression import check_table_suppression
from code.check_field_suppression import (check_field_suppression, check_vehicle_accident_suppression,
                 check_field_cancer_concept_suppression, check_field_freetext_response_suppression)
from code.check_concept_suppression import check_concept_suppression
from code.check_mapping import check_mapping, check_site_mapping, check_mapping_zipcode_generalization, check_mapping_zipcode_transformation

# funtions from utils
from utils.helpers import highlight, load_check_description, load_tables_for_check, filter_data_by_rule

import logging
import sys

# config log
logging.basicConfig(format='%(asctime)s [%(levelname)s] - %(message)s', level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger()


def run_qc(project_id, post_deid_dataset, pre_deid_dataset, mapping_dataset=None, rule_code=None):
    list_checks = load_check_description(rule_code)
    list_checks = list_checks[list_checks['level'].notnull()].copy()

    check_dict = load_tables_for_check()
    
    checks = []
    for _, row in list_checks.iterrows():
        rule = row['rule']
        logger.info(f"Running {rule} - {row['description']}")

        check_level = row['level']
        check_file = check_dict.get(check_level)
        check_df = filter_data_by_rule(check_file, rule)
        check_function = eval(row['code'])
        df = check_function(check_df, project_id, post_deid_dataset, pre_deid_dataset, mapping_dataset)
        checks.append(df)
    return pd.concat(checks, sort=True).reset_index(drop=True)
      

def display_check_summary_by_rule(checks_df):
    by_rule = checks_df.groupby('rule')['n_row_violation'].sum().reset_index()
    needed_description_columns = ['rule', 'description']
    check_description = (load_check_description()
                            .filter(items=needed_description_columns)
                        )
    if not by_rule.empty:
        by_rule = by_rule.merge(check_description, how='inner', on='rule')
    else:
        by_rule = check_description.copy()
        by_rule['n_row_violation'] = 0
    by_rule['n_row_violation'] = by_rule['n_row_violation'].fillna(0).astype(int)
    return by_rule.style.apply(highlight, axis=1)

    
def display_check_detail_of_rule(checks_df, rule):
    return checks_df[checks_df['rule'] == rule].dropna(axis=1, how='all') if rule in checks_df['rule'].values else 'Nothing to report'