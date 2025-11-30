from mock import patch
from athenacli.config import AWSConfig
from configobj import ConfigObj


def test_result_reuse_defaults():
    """Test that result reuse has correct defaults."""
    config = ConfigObj()
    aws_config = AWSConfig(
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region=None,
        s3_staging_dir=None,
        work_group=None,
        profile='default',
        config=config
    )
    
    assert aws_config.result_reuse_enable is False
    assert aws_config.result_reuse_minutes == 60


def test_result_reuse_from_cli():
    """Test that CLI parameters override defaults."""
    config = ConfigObj()
    aws_config = AWSConfig(
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region=None,
        s3_staging_dir=None,
        work_group=None,
        profile='default',
        config=config,
        result_reuse_enable=True,
        result_reuse_minutes=120
    )
    
    assert aws_config.result_reuse_enable is True
    assert aws_config.result_reuse_minutes == 120


def test_result_reuse_from_config_file():
    """Test that config file values are used."""
    config = ConfigObj()
    config['aws_profile default'] = {
        'aws_access_key_id': None,
        'aws_secret_access_key': None,
        'region': None,
        's3_staging_dir': None,
        'work_group': None,
        'result_reuse_enable': 'true',
        'result_reuse_minutes': '90'
    }
    
    with patch('athenacli.config.boto3'):
        aws_config = AWSConfig(
            aws_access_key_id=None,
            aws_secret_access_key=None,
            region=None,
            s3_staging_dir=None,
            work_group=None,
            profile='default',
            config=config
        )
    
    assert aws_config.result_reuse_enable is True
    assert aws_config.result_reuse_minutes == 90


def test_result_reuse_cli_overrides_config():
    """Test that CLI parameters override config file."""
    config = ConfigObj()
    config['aws_profile default'] = {
        'aws_access_key_id': None,
        'aws_secret_access_key': None,
        'region': None,
        's3_staging_dir': None,
        'work_group': None,
        'result_reuse_enable': 'false',
        'result_reuse_minutes': '30'
    }
    
    with patch('athenacli.config.boto3'):
        aws_config = AWSConfig(
            aws_access_key_id=None,
            aws_secret_access_key=None,
            region=None,
            s3_staging_dir=None,
            work_group=None,
            profile='default',
            config=config,
            result_reuse_enable=True,
            result_reuse_minutes=180
        )
    
    assert aws_config.result_reuse_enable is True
    assert aws_config.result_reuse_minutes == 180


def test_get_bool_parsing():
    """Test boolean parsing from strings."""
    config = ConfigObj()
    aws_config = AWSConfig(
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region=None,
        s3_staging_dir=None,
        work_group=None,
        profile='default',
        config=config
    )
    
    # Test various truthy strings
    assert aws_config.get_bool('true') is True
    assert aws_config.get_bool('True') is True
    assert aws_config.get_bool('1') is True
    assert aws_config.get_bool('yes') is True
    assert aws_config.get_bool('on') is True
    
    # Test falsy strings
    assert aws_config.get_bool('false') is False
    assert aws_config.get_bool('0') is False
    assert aws_config.get_bool('no') is False
    
    # Test actual booleans
    assert aws_config.get_bool(True) is True
    assert aws_config.get_bool(False) is False


def test_get_int_parsing():
    """Test integer parsing from strings."""
    config = ConfigObj()
    aws_config = AWSConfig(
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region=None,
        s3_staging_dir=None,
        work_group=None,
        profile='default',
        config=config
    )
    
    # Test string to int conversion
    assert aws_config.get_int('60') == 60
    assert aws_config.get_int('120') == 120
    
    # Test actual int
    assert aws_config.get_int(90) == 90
    
    # Test invalid values fall back to default
    assert aws_config.get_int('invalid') == 60
    assert aws_config.get_int(None) == 60
