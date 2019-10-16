# +
import bq
from defaults import DEFAULT_DATASETS
import render
from parameters import SANDBOX

DEID = DEFAULT_DATASETS.latest.deid
COMBINED = DEFAULT_DATASETS.latest.combined

print("""DEID={DEID}
COMBINED={COMBINED}""".format(DEID=DEID, COMBINED=COMBINED))
# -


INVALID_VISIT_ID_QUERY = """
SELECT 
 t.{TABLE}_id, 
 t.visit_occurrence_id
FROM {DATASET}.{TABLE} t
WHERE t.visit_occurrence_id IS NOT NULL
AND NOT EXISTS 
(SELECT 1 
 FROM {DATASET}.visit_occurrence v
 WHERE v.visit_occurrence_id = t.visit_occurrence_id)"""

import resources
import common


def get_tables_with_visit_id():
    tables_with_visit_id = []
    for table in resources.CDM_TABLES:
        if table == 'visit_occurrence':
            continue
        fields = resources.fields_for(table)
        for field in fields:
            if field['name'] == 'visit_occurrence_id':
                tables_with_visit_id.append(table)
    return tables_with_visit_id

# +
LOG_MESSAGE_FMT = '{ROW_COUNT} records to delete from {DATASET}.{TABLE} logged in {SANDBOX}.{DEST_TABLE}'

for table in get_tables_with_visit_id():
    q = INVALID_VISIT_ID_QUERY.format(DATASET=DEID, TABLE=table)
    df = bq.query(q)
    dest_table = '{SANDBOX}.{DATASET}_invalid_visit_id_{TABLE}'.format(SANDBOX=SANDBOX, DATASET=DEID, TABLE=table)
    df.to_gbq(destination_table=dest_table, if_exists='replace')
    print(LOG_MESSAGE_FMT.format(ROW_COUNT=len(df), TABLE=table, DATASET=DEID, SANDBOX=SANDBOX, DEST_TABLE=dest_table))
# -

for table in get_tables_with_visit_id():
    q = INVALID_VISIT_ID_QUERY.format(DATASET=DEID, TABLE=table)
    df = bq.query(q)
    print('{TABLE},{ROW_COUNT}'.format(TABLE=table, ROW_COUNT=len(df)))
