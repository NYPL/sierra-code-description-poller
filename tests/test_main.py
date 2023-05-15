import main
import pytest

from tests.test_helpers import TestHelpers


_TEST_SIERRA_RESPONSES = [
    [
        {'code': 1, 'description': 'ptype 1'},
        {'code': 2, 'description': 'ptype 2'},
        {'code': 3, 'description': 'ptype 3'}
    ],
    [
        {'code': 4, 'description': 'pcode 4'},
        {'code': 5, 'description': 'pcode 5'},
        {'code': 6, 'description': 'pcode 6'}
    ],
    [
        {'code': 7, 'description': 'itype 7'},
        {'code': 8, 'description': 'itype 8'},
        {'code': 9, 'description': 'itype 9'}
    ],
    [
        {'code': 'a', 'description': 'item status a'},
        {'code': 'b', 'description': 'item status b'},
        {'code': 'c', 'description': 'item status c'}
    ],
    [
        {'terminal_code': 10, 'location_code': 'dd',
            'description': 'stat group 10'},
        {'terminal_code': 11, 'location_code': 'dd',
            'description': 'stat group 11'},
        {'terminal_code': 12, 'location_code': 'ee',
            'description': 'stat group 12'}
    ],
    [
        {'location_code': 'dd', 'branch_name': 'Library D',
            'description': 'Library D children\'s'},
        {'location_code': 'ee', 'branch_name': 'Library E',
            'description': 'Library E fiction'},
        {'location_code': 'ff', 'branch_name': 'Library F',
            'description': 'Library F adult learning'}
    ]
]


class TestMain:

    @classmethod
    def setup_class(cls):
        TestHelpers.set_env_vars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clear_env_vars()

    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch('main.load_env_file')
        mocker.patch('main.create_log')

    @pytest.fixture
    def mock_sierra_client(self, mocker):
        mock_sierra_client = mocker.MagicMock()
        mock_sierra_client.execute_query.side_effect = _TEST_SIERRA_RESPONSES
        mocker.patch('main.PostgreSQLClient', return_value=mock_sierra_client)
        return mock_sierra_client

    def test_main(self, test_instance, mock_sierra_client, mocker):
        mock_avro_encoder = mocker.MagicMock()
        mock_avro_encoder.encode_batch.return_value = [b'1', b'2', b'3']
        mock_avro_encoder_constructor = mocker.patch(
            'main.AvroEncoder', return_value=mock_avro_encoder)

        mock_kinesis_client = mocker.MagicMock()
        mock_kinesis_client_constructor = mocker.patch(
            'main.KinesisClient', return_value=mock_kinesis_client)

        main.main()

        assert mock_sierra_client.connect.call_count == 6
        assert mock_sierra_client.close_connection.call_count == 6
        mock_sierra_client.execute_query.assert_has_calls([
            mocker.call(
                'SELECT value AS code, name AS description FROM '
                'sierra_view.ptype_property_myuser;'),
            mocker.call(
                'SELECT code, name AS description FROM '
                'sierra_view.user_defined_pcode3_myuser;'),
            mocker.call(
                'SELECT code, name AS description FROM '
                'sierra_view.itype_property_myuser;'),
            mocker.call(
                'SELECT code, name AS description FROM '
                'sierra_view.item_status_property_myuser;'),
            mocker.call(
                'SELECT code AS terminal_code, location_code, name AS '
                'description FROM sierra_view.statistic_group_myuser;'),
            mocker.call(
                '''
        SELECT DISTINCT location.code AS location_code, branch_myuser.name AS
            branch_name, location_name.name AS description
        FROM sierra_view.location
        LEFT JOIN sierra_view.branch_myuser
            ON location.branch_code_num = branch_myuser.code
        LEFT JOIN sierra_view.location_name
            ON location.id = location_name.location_id;''')
        ])

        mock_avro_encoder_constructor.assert_has_calls([
            mocker.call('https://test_schema_url/PtypeCode'),
            mocker.call('https://test_schema_url/Pcode3Code'),
            mocker.call('https://test_schema_url/ItypeCode'),
            mocker.call('https://test_schema_url/ItemStatusCode'),
            mocker.call('https://test_schema_url/StatGroupCode'),
            mocker.call('https://test_schema_url/LocationCode')
        ])
        mock_avro_encoder.encode_batch.assert_has_calls(
            [mocker.call(response) for response in _TEST_SIERRA_RESPONSES])

        mock_kinesis_client_constructor.assert_has_calls([
            mocker.call('test_kinesis_stream:PtypeCode-test', 2),
            mocker.call('test_kinesis_stream:Pcode3Code-test', 2),
            mocker.call('test_kinesis_stream:ItypeCode-test', 2),
            mocker.call('test_kinesis_stream:ItemStatusCode-test', 2),
            mocker.call('test_kinesis_stream:StatGroupCode-test', 2),
            mocker.call('test_kinesis_stream:LocationCode-test', 2)
        ])
        mock_kinesis_client.send_records.assert_has_calls(
            [mocker.call([b'1', b'2', b'3'])]*6)
        assert mock_kinesis_client.close.call_count == 6
