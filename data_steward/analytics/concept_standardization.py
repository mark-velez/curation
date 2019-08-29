# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 2
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython2
#     version: 2.7.15+
# ---

# +
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

def query(q):
    return client.query(q).to_dataframe()

PRIMARY_CONCEPT_FIELDS = [
    dict(table='condition_occurrence', field='condition_concept_id'),
    dict(table='procedure_occurrence', field='procedure_concept_id'),
    dict(table='drug_exposure', field='drug_concept_id'),
    dict(table='device_exposure', field='device_concept_id'),
    dict(table='observation', field='observation_concept_id'),
    dict(table='measurement', field='measurement_concept_id'),
    dict(table='visit_occurrence', field='visit_concept_id')
]
# -

PROPORTION_QUERY = '''
(WITH distinct_concept AS 
(SELECT DISTINCT {field} concept_id FROM {dataset_id}.{table})

SELECT 
 '{dataset_id}' as dataset_id,
 '{table}' as table,
 '{field}' as field,
 dc.concept_id AS concept_id,
 IF(c.concept_id IS NULL, 0, 1) AS is_valid,
 CASE c.standard_concept
   WHEN 'S' THEN 1
   ELSE 0
 END AS is_standard
FROM distinct_concept dc
LEFT JOIN {dataset_id}.concept c 
 ON dc.concept_id = c.concept_id)
'''

qs = []
for primary_concept_field in PRIMARY_CONCEPT_FIELDS:
    q1 = PROPORTION_QUERY.format(dataset_id='combined20190802_base', **primary_concept_field)
    q2 = PROPORTION_QUERY.format(dataset_id='combined20190802_std', **primary_concept_field)
    qs.extend([q1, q2])
q = '\nUNION ALL\n'.join(qs)

df = query(q)

df2 = df.groupby(['dataset_id','field']).agg({'is_standard':'sum','is_valid':'sum'}).reset_index().sort_values(by=['field', 'dataset_id'])

df2.pivot(columns='dataset_id')

g = df2.groupby(['field'])

g.describe()

df2

