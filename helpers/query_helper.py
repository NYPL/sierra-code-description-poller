QUERY_MAP = {
    'sierra_ptype_codes':
        'SELECT value, TRIM(name) FROM sierra_view.ptype_property_myuser;',
    'sierra_pcode3_codes':
        ('SELECT TRIM(code), TRIM(name) '
         'FROM sierra_view.user_defined_pcode3_myuser;'),
    'sierra_itype_codes':
        'SELECT code, TRIM(name) FROM sierra_view.itype_property_myuser;',
    'sierra_item_status_codes':
        ('SELECT TRIM(code), TRIM(name) '
         'FROM sierra_view.item_status_property_myuser;'),
    'sierra_stat_group_codes':
        ('SELECT code, TRIM(location_code), TRIM(name) '
         'FROM sierra_view.statistic_group_myuser;'),
    'sierra_location_codes': '''
SELECT TRIM(location_myuser.code), TRIM(location_myuser.name),
    TRIM(branch_myuser.name)
FROM sierra_view.location_myuser
LEFT JOIN sierra_view.branch_myuser
    ON location_myuser.branch_code_num = branch_myuser.code;'''
}

COLUMNS_MAP = {
    'sierra_ptype_codes': ['code', 'description'],
    'sierra_pcode3_codes': ['code', 'description'],
    'sierra_itype_codes': ['code', 'description'],
    'sierra_item_status_codes': ['code', 'description'],
    'sierra_stat_group_codes':
        ['terminal_code', 'location_code', 'description'],
    'sierra_location_codes': ['location_code', 'description', 'branch_name']
}

STRING_CODES = {'sierra_item_status_codes', 'sierra_location_codes'}

_REDSHIFT_SELECT_QUERY = \
    'SELECT {columns} FROM {table} WHERE deletion_date IS NULL;'

_REDSHIFT_UPDATE_QUERY = '''
UPDATE {table} SET deletion_date='{today}'
WHERE {code_column_name} IN ({deprecated_codes});'''

_REDSHIFT_INSERT_QUERY = 'INSERT INTO {table} VALUES ({placeholder});'


def build_redshift_select_query(table, column_names):
    return _REDSHIFT_SELECT_QUERY.format(
        columns=', '.join(column_names), table=table)


def build_redshift_update_query(table, today, code_column_name,
                                deprecated_codes):
    return _REDSHIFT_UPDATE_QUERY.format(
        table=table, today=today,
        code_column_name=code_column_name,
        deprecated_codes=deprecated_codes)


def build_redshift_insert_query(table, row_length):
    placeholder = ", ".join(["%s"] * row_length)
    return _REDSHIFT_INSERT_QUERY.format(
        table=table, placeholder=placeholder)
