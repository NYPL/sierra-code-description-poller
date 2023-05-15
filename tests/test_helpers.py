import os


class TestHelpers:
    ENV_VARS = {
        'ENVIRONMENT': 'test',
        'AWS_REGION': 'test_aws_region',
        'BASE_SCHEMA_URL': 'https://test_schema_url/',
        'KINESIS_BATCH_SIZE': '2',
        'BASE_KINESIS_STREAM_ARN': 'test_kinesis_stream:',
        'SIERRA_DB_PORT': 'test_sierra_port',
        'SIERRA_DB_NAME': 'test_sierra_name',
        'SIERRA_DB_HOST': 'test_sierra_host',
        'SIERRA_DB_USER': 'test_sierra_user',
        'SIERRA_DB_PASSWORD': 'test_sierra_password'
    }

    @classmethod
    def set_env_vars(cls):
        for key, value in cls.ENV_VARS.items():
            os.environ[key] = value

    @classmethod
    def clear_env_vars(cls):
        for key in cls.ENV_VARS.keys():
            if key in os.environ:
                del os.environ[key]
