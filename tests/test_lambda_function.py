import lambda_function
import pytest

from freezegun import freeze_time
from tests.test_helpers import TestHelpers


_TEST_SIERRA_RESPONSES = [
    [(1, 'ptype 1'), (2, 'ptype 2'), (3, 'ptype 3')],
    [('4', 'pcode 4'), ('5', 'pcode 5'), ('6', 'pcode 6')],
    [(7, 'itype 7'), (8, 'itype 8'), (9, 'itype 9')],
    [('a', 'item status a'), ('b', 'item status b'), ('c', 'item status c')],
    [
        (10, 'dd', 'stat group 10'),
        (11, 'dd', 'stat group 11'),
        (12, 'ee', 'stat group 12'),
        (13, 'ff', 'stat group 13')
    ],
    [
        ('dd', 'Library D children\'s', 'Library D'),
        ('ee', 'Library E fiction', 'Library E'),
        ('ff', 'Library F adult learning', 'Library F'),
        ('gg', 'Library G off-site storage', 'Library G')
    ]
]

_TEST_REDSHIFT_RESPONSES = [
    ([2, 'ptype 2x'], [3, 'ptype 3'], [4, 'ptype 4']),
    ([5, 'pcode 5x'], [6, 'pcode 6'], [7, 'pcode 7']),
    ([8, 'itype 8x'], [9, 'itype 9'], [10, 'itype 10']),
    (['b', 'item status bx'], ['c', 'item status c'], ['d', 'item status d']),
    (
        [11, 'ddx', 'stat group 11'],
        [12, 'ee', 'stat group 12x'],
        [13, 'ff', 'stat group 13'],
        [14, 'gg', 'stat group 14']
    ),
    (
        ['ee', 'Library E old', 'Library E'],
        ['ff', 'Library F adult learning', 'Library F old'],
        ['gg', 'Library G off-site storage', 'Library G'],
        ['hh', 'Library H periodicals', 'Library H']
    )
]

_TEST_INSERT_ROWS = [
    [[1, 'ptype 1', '2023-01-01'], [2, 'ptype 2', '2023-01-01']],
    [[4, 'pcode 4', '2023-01-01'], [5, 'pcode 5', '2023-01-01']],
    [[7, 'itype 7', '2023-01-01'], [8, 'itype 8', '2023-01-01']],
    [
        ['a', 'item status a', '2023-01-01'],
        ['b', 'item status b', '2023-01-01']],
    [
        [10, 'dd', 'stat group 10', '2023-01-01'],
        [11, 'dd', 'stat group 11', '2023-01-01'],
        [12, 'ee', 'stat group 12', '2023-01-01']
    ],
    [
        ['dd', 'Library D children\'s', 'Library D', '2023-01-01'],
        ['ee', 'Library E fiction', 'Library E', '2023-01-01'],
        ['ff', 'Library F adult learning', 'Library F', '2023-01-01']
    ]
]


@freeze_time('2023-01-01 01:23:45-05:00')
class TestLambdaFunction:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('lambda_function.create_log')
        mocker.patch('lambda_function.load_env_file')

        mock_kms_client = mocker.MagicMock()
        mock_kms_client.decrypt.return_value = 'decrypted'
        mocker.patch('lambda_function.KmsClient', return_value=mock_kms_client)

    @pytest.fixture
    def mock_sierra_client(self, mocker):
        mock_sierra_client = mocker.MagicMock()
        mock_sierra_client.execute_query.side_effect = _TEST_SIERRA_RESPONSES
        mocker.patch('lambda_function.PostgreSQLClient',
                     return_value=mock_sierra_client)
        return mock_sierra_client

    @pytest.fixture
    def mock_redshift_client(self, mocker):
        mock_redshift_client = mocker.MagicMock()
        mock_redshift_client.execute_query.side_effect = \
            _TEST_REDSHIFT_RESPONSES
        mocker.patch('lambda_function.RedshiftClient',
                     return_value=mock_redshift_client)
        return mock_redshift_client

    def test_lambda_handler(self, test_instance, mock_sierra_client,
                            mock_redshift_client, mocker):
        mock_redshift_select_query = mocker.patch(
            'lambda_function.build_redshift_select_query',
            return_value='REDSHIFT SELECT QUERY')
        mock_redshift_insert_query = mocker.patch(
            'lambda_function.build_redshift_insert_query',
            return_value='REDSHIFT INSERT QUERY')
        mock_redshift_update_query = mocker.patch(
            'lambda_function.build_redshift_update_query',
            return_value='REDSHIFT UPDATE QUERY')
        lambda_function.lambda_handler(None, None)

        assert mock_sierra_client.connect.call_count == 6
        assert mock_sierra_client.close_connection.call_count == 6
        mock_sierra_client.execute_query.assert_has_calls([
            mocker.call(
                'SELECT value, name FROM sierra_view.ptype_property_myuser;'),
            mocker.call(
                'SELECT code, name FROM '
                'sierra_view.user_defined_pcode3_myuser;'),
            mocker.call(
                'SELECT code, name FROM sierra_view.itype_property_myuser;'),
            mocker.call(
                'SELECT code, name FROM '
                'sierra_view.item_status_property_myuser;'),
            mocker.call(
                'SELECT code, location_code, name FROM '
                'sierra_view.statistic_group_myuser;'),
            mocker.call(
                '''
SELECT location_myuser.code, location_myuser.name, branch_myuser.name
FROM sierra_view.location_myuser
LEFT JOIN sierra_view.branch_myuser
    ON location_myuser.branch_code_num = branch_myuser.code;''')
        ])

        assert mock_redshift_client.connect.call_count == 6
        assert mock_redshift_client.execute_query.call_count == 6
        assert mock_redshift_client.close_connection.call_count == 6

        mock_redshift_select_query.assert_has_calls([
            mocker.call('sierra_ptype_codes_test_db', ['code', 'description']),
            mocker.call('sierra_pcode3_codes_test_db',
                        ['code', 'description']),
            mocker.call('sierra_itype_codes_test_db', ['code', 'description']),
            mocker.call('sierra_item_status_codes_test_db',
                        ['code', 'description']),
            mocker.call('sierra_stat_group_codes_test_db',
                        ['terminal_code', 'location_code', 'description']),
            mocker.call('sierra_location_codes_test_db',
                        ['location_code', 'description', 'branch_name'])
        ])
        mock_redshift_insert_query.assert_has_calls([
            mocker.call('sierra_ptype_codes_test_db', 3),
            mocker.call('sierra_pcode3_codes_test_db', 3),
            mocker.call('sierra_itype_codes_test_db', 3),
            mocker.call('sierra_item_status_codes_test_db', 3),
            mocker.call('sierra_stat_group_codes_test_db', 4),
            mocker.call('sierra_location_codes_test_db', 4)
        ])
        mock_redshift_update_query.assert_has_calls([
            mocker.call('sierra_ptype_codes_test_db',
                        '2023-01-01', 'code', "2,4"),
            mocker.call('sierra_pcode3_codes_test_db',
                        '2023-01-01', 'code', "5,7"),
            mocker.call('sierra_itype_codes_test_db',
                        '2023-01-01', 'code', "8,10"),
            mocker.call('sierra_item_status_codes_test_db',
                        '2023-01-01', 'code', "'b','d'"),
            mocker.call('sierra_stat_group_codes_test_db',
                        '2023-01-01', 'terminal_code', "11,12,14"),
            mocker.call('sierra_location_codes_test_db',
                        '2023-01-01', 'location_code', "'ee','ff','hh'"),
        ])

        args_list = mock_redshift_client.execute_transaction.call_args_list
        for i in range(len(args_list)):
            transaction_args = args_list[i][0][0]
            assert transaction_args[0] == ('REDSHIFT UPDATE QUERY', None)
            for j in range(1, len(transaction_args)):
                assert transaction_args[j][0] == 'REDSHIFT INSERT QUERY'
                assert list(transaction_args[j][1]) == \
                    _TEST_INSERT_ROWS[i][j-1]

    def test_lambda_handler_no_sierra_response(
            self, test_instance, mock_redshift_client, mocker):
        mock_sierra_client = mocker.MagicMock()
        mock_sierra_client.execute_query.return_value = []
        mocker.patch('lambda_function.PostgreSQLClient',
                     return_value=mock_sierra_client)

        with pytest.raises(lambda_function.SierraCodeDescriptionPollerError):
            lambda_function.lambda_handler(None, None)

        mock_sierra_client.connect.assert_called_once()
        mock_sierra_client.execute_query.assert_called_once()
        mock_sierra_client.close_connection.assert_called_once()

        mock_redshift_client.connect.assert_not_called()
        mock_redshift_client.execute_query.assert_not_called()
        mock_redshift_client.execute_transaction.assert_not_called()

    def test_lambda_handler_no_deprecated_codes(
            self, test_instance, mock_sierra_client, mocker):
        _REDSHIFT_RESPONSES = [(x[-2],) for x in _TEST_REDSHIFT_RESPONSES]

        mock_redshift_client = mocker.MagicMock()
        mock_redshift_client.execute_query.side_effect = _REDSHIFT_RESPONSES
        mocker.patch('lambda_function.RedshiftClient',
                     return_value=mock_redshift_client)

        mocker.patch('lambda_function.build_redshift_select_query',
                     return_value='REDSHIFT SELECT QUERY')
        mock_redshift_insert_query = mocker.patch(
            'lambda_function.build_redshift_insert_query',
            return_value='REDSHIFT INSERT QUERY')
        mock_redshift_update_query = mocker.patch(
            'lambda_function.build_redshift_update_query',
            return_value='REDSHIFT UPDATE QUERY')

        lambda_function.lambda_handler(None, None)

        mock_redshift_update_query.assert_not_called()
        assert mock_redshift_insert_query.call_count == 6
        assert mock_redshift_client.execute_transaction.call_count == 6
        assert mock_redshift_client.close_connection.call_count == 6

    def test_lambda_handler_no_fresh_codes(
            self, test_instance, mock_redshift_client, mocker):
        _SIERRA_RESPONSES = [[x[-1],] for x in _TEST_SIERRA_RESPONSES]

        mock_sierra_client = mocker.MagicMock()
        mock_sierra_client.execute_query.side_effect = _SIERRA_RESPONSES
        mocker.patch('lambda_function.PostgreSQLClient',
                     return_value=mock_sierra_client)

        mocker.patch('lambda_function.build_redshift_select_query',
                     return_value='REDSHIFT SELECT QUERY')
        mock_redshift_insert_query = mocker.patch(
            'lambda_function.build_redshift_insert_query',
            return_value='REDSHIFT INSERT QUERY')
        mock_redshift_update_query = mocker.patch(
            'lambda_function.build_redshift_update_query',
            return_value='REDSHIFT UPDATE QUERY')

        lambda_function.lambda_handler(None, None)

        mock_redshift_insert_query.assert_not_called()
        assert mock_redshift_update_query.call_count == 6
        assert mock_redshift_client.execute_transaction.call_count == 6
        assert mock_redshift_client.close_connection.call_count == 6

    def test_lambda_handler_no_code_changes(self, test_instance, mocker):
        _SIERRA_RESPONSES = [[x[-1],] for x in _TEST_SIERRA_RESPONSES]
        _REDSHIFT_RESPONSES = [(x[-2],) for x in _TEST_REDSHIFT_RESPONSES]

        mock_sierra_client = mocker.MagicMock()
        mock_sierra_client.execute_query.side_effect = _SIERRA_RESPONSES
        mocker.patch('lambda_function.PostgreSQLClient',
                     return_value=mock_sierra_client)

        mock_redshift_client = mocker.MagicMock()
        mock_redshift_client.execute_query.side_effect = _REDSHIFT_RESPONSES
        mocker.patch('lambda_function.RedshiftClient',
                     return_value=mock_redshift_client)

        mocker.patch('lambda_function.build_redshift_select_query',
                     return_value='REDSHIFT SELECT QUERY')
        mock_redshift_insert_query = mocker.patch(
            'lambda_function.build_redshift_insert_query',
            return_value='REDSHIFT INSERT QUERY')
        mock_redshift_update_query = mocker.patch(
            'lambda_function.build_redshift_update_query',
            return_value='REDSHIFT UPDATE QUERY')

        lambda_function.lambda_handler(None, None)

        mock_redshift_insert_query.assert_not_called()
        mock_redshift_update_query.assert_not_called()
        mock_redshift_client.execute_transaction.assert_not_called()
        assert mock_redshift_client.close_connection.call_count == 6
