import os

from nypl_py_utils.classes.avro_encoder import AvroEncoder
from nypl_py_utils.classes.kinesis_client import KinesisClient
from nypl_py_utils.classes.postgresql_client import PostgreSQLClient
from nypl_py_utils.functions.config_helper import load_env_file
from nypl_py_utils.functions.log_helper import create_log
from psycopg.rows import dict_row

_QUERY_MAP = {
    'PtypeCode': ('SELECT value AS code, name AS description FROM '
                  'sierra_view.ptype_property_myuser;'),
    'Pcode3Code': ('SELECT code, name AS description FROM '
                   'sierra_view.user_defined_pcode3_myuser;'),
    'ItypeCode': ('SELECT code, name AS description FROM '
                  'sierra_view.itype_property_myuser;'),
    'ItemStatusCode': ('SELECT code, name AS description FROM '
                       'sierra_view.item_status_property_myuser;'),
    'StatGroupCode':
        ('SELECT code AS terminal_code, location_code, name AS description '
         'FROM sierra_view.statistic_group_myuser;'),
    'LocationCode': '''
        SELECT DISTINCT location.code AS location_code, branch_myuser.name AS
            branch_name, location_name.name AS description
        FROM sierra_view.location
        LEFT JOIN sierra_view.branch_myuser
            ON location.branch_code_num = branch_myuser.code
        LEFT JOIN sierra_view.location_name
            ON location.id = location_name.location_id;''',
}


def main():
    load_env_file(os.environ['ENVIRONMENT'], 'config/{}.yaml')
    logger = create_log(__name__)
    sierra_client = PostgreSQLClient(
        os.environ['SIERRA_DB_HOST'], os.environ['SIERRA_DB_PORT'],
        os.environ['SIERRA_DB_NAME'], os.environ['SIERRA_DB_USER'],
        os.environ['SIERRA_DB_PASSWORD'])

    for schema_name, query in _QUERY_MAP.items():
        logger.info('Begin {} run'.format(schema_name))
        avro_encoder = AvroEncoder(os.environ['BASE_SCHEMA_URL'] + schema_name)

        sierra_client.connect(row_factory=dict_row)
        sierra_data = sierra_client.execute_query(query)
        sierra_client.close_connection()
        encoded_records = avro_encoder.encode_batch(sierra_data)

        if os.environ.get('IGNORE_KINESIS', False) != 'True':
            kinesis_stream_arn = os.environ['BASE_KINESIS_STREAM_ARN'] + \
                schema_name + '-{}'.format(os.environ['ENVIRONMENT'])
            kinesis_client = KinesisClient(
                kinesis_stream_arn, int(os.environ['KINESIS_BATCH_SIZE']))
            kinesis_client.send_records(encoded_records)
            kinesis_client.close()


if __name__ == '__main__':
    main()
