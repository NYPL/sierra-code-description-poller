import json
import os
import pandas as pd
import pytz

from datetime import datetime
from helpers.query_helper import (
    build_redshift_insert_query, build_redshift_select_query,
    build_redshift_update_query, COLUMNS_MAP, QUERY_MAP, STRING_CODES)
from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.functions.config_helper import load_env_file
from nypl_py_utils.functions.log_helper import create_log


def lambda_handler(event, context):
    if os.environ['ENVIRONMENT'] == 'devel':
        load_env_file('devel', 'config/{}.yaml')
    logger = create_log('lambda_function')
    logger.info('Starting lambda processing')

    kms_client = KmsClient()
    sierra_client = PostgreSQLClient(
        kms_client.decrypt(os.environ['SIERRA_DB_HOST']),
        os.environ['SIERRA_DB_PORT'],
        os.environ['SIERRA_DB_NAME'],
        kms_client.decrypt(os.environ['SIERRA_DB_USER']),
        kms_client.decrypt(os.environ['SIERRA_DB_PASSWORD']))
    redshift_client = RedshiftClient(
        kms_client.decrypt(os.environ['REDSHIFT_DB_HOST']),
        os.environ['REDSHIFT_DB_NAME'],
        kms_client.decrypt(os.environ['REDSHIFT_DB_USER']),
        kms_client.decrypt(os.environ['REDSHIFT_DB_PASSWORD']))
    kms_client.close()

    today = datetime.now(pytz.timezone('US/Eastern')).date().isoformat()
    for id, query in QUERY_MAP.items():
        logger.info('Beginning {} run'.format(id))
        column_names = COLUMNS_MAP[id]
        code_column_name = column_names[0]
        if os.environ['REDSHIFT_DB_NAME'] == 'production':
            redshift_table = id
        else:
            redshift_table = id + '_' + os.environ['REDSHIFT_DB_NAME']

        # Query Sierra for fresh codes/descriptions
        sierra_client.connect()
        raw_sierra_data = sierra_client.execute_query(query)
        sierra_client.close_connection()
        if len(raw_sierra_data) == 0:
            raise SierraCodeDescriptionPollerError(
                'No data found in Sierra for {} query'.format(id))
        sierra_df = pd.DataFrame(data=raw_sierra_data, dtype='string',
                                 columns=column_names)
        sierra_df['creation_date'] = today

        # Query Redshift for the codes/descriptions we currently have stored
        redshift_client.connect()
        raw_redshift_data = redshift_client.execute_query(
            build_redshift_select_query(redshift_table, column_names))
        redshift_df = pd.DataFrame(data=raw_redshift_data, dtype='string',
                                   columns=column_names)

        # Get the new or updated codes that need to be added to Redshift and
        # the deprecated codes that need to be marked as deleted in Redshift.
        # Note that a modification to an existing code's description results
        # in the existing record in Redshift being marked as deleted and a new
        # record with the updated description being added.
        all_codes_df = pd.merge(sierra_df, redshift_df, how='outer',
                                indicator='merge_status')
        fresh_codes_df = all_codes_df[
            all_codes_df['merge_status'] == 'left_only'].copy()
        deprecated_codes = all_codes_df[
            all_codes_df['merge_status'] == 'right_only'][code_column_name]
        if id in STRING_CODES:
            deprecated_codes_str = "'" + "','".join(deprecated_codes) + "'"
        else:
            fresh_codes_df[code_column_name] = pd.to_numeric(
                fresh_codes_df[code_column_name], errors='coerce'
            ).astype('Int64')
            deprecated_codes_str = ','.join(deprecated_codes)

        # In a single Redshift transaction, update the deprecated codes and
        # insert the new ones
        queries = []
        if len(deprecated_codes) > 0:
            queries.append(
                (build_redshift_update_query(
                    redshift_table, today, code_column_name,
                    deprecated_codes_str), None))

        if len(fresh_codes_df) > 0:
            insert_query = build_redshift_insert_query(
                redshift_table, fresh_codes_df.shape[1] - 1)
            for row in fresh_codes_df.values:
                row = [x if pd.notnull(x) else None for x in row]
                queries.append((insert_query, row[:-1]))

        if len(queries) > 0:
            redshift_client.execute_transaction(queries)
        redshift_client.close_connection()
        logger.info('Finished {} run'.format(id))

    logger.info('Finished lambda processing')
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Job ran successfully."
        })
    }


class SierraCodeDescriptionPollerError(Exception):
    def __init__(self, message=None):
        self.message = message
