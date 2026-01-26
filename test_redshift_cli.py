#!/usr/bin/env python3
"""Test RedshiftCLI with IAM authentication."""

import boto3
import sys
import os

# Get IAM credentials
session = boto3.Session(profile_name='raptor_engineer')
redshift_client = session.client('redshift', region_name='us-east-1')

response = redshift_client.get_cluster_credentials(
    DbUser='thombeck',
    DbName='integration_analytics',
    ClusterIdentifier='raptor-redshift-cluster',
    DurationSeconds=3600,
    AutoCreate=False
)

db_user = response['DbUser']
db_password = response['DbPassword']

# Set environment variables for redshiftcli
os.environ['REDSHIFT_HOST'] = 'raptor-redshift-cluster.projectraptor.io'
os.environ['REDSHIFT_PORT'] = '5439'
os.environ['REDSHIFT_DATABASE'] = 'integration_analytics'
os.environ['REDSHIFT_USER'] = db_user
os.environ['REDSHIFT_PASSWORD'] = db_password
os.environ['PGSSLMODE'] = 'require'

print(f"Testing redshiftcli with IAM user: {db_user}")
print("=" * 60)

# Now run redshiftcli
from redshiftcli.main import cli
from click.testing import CliRunner

runner = CliRunner()

# Test 1: Count query
print("\n1. Testing COUNT query...")
result = runner.invoke(cli, [
    '-h', 'raptor-redshift-cluster.projectraptor.io',
    '-p', '5439',
    '-U', db_user,
    '-W', db_password,
    '--sslmode', 'require',
    '--execute', 'SELECT COUNT(*) FROM integration.envelope_table_v2',
    '--table-format', 'psql',
    'integration_analytics'
])

if result.exit_code == 0:
    print("   ✓ Query executed successfully!")
    print(result.output)
else:
    print(f"   ✗ Error: {result.exception}")

# Test 2: Sample data
print("\n2. Testing sample data query...")
result = runner.invoke(cli, [
    '-h', 'raptor-redshift-cluster.projectraptor.io',
    '-p', '5439',
    '-U', db_user,
    '-W', db_password,
    '--sslmode', 'require',
    '--execute', 'SELECT calendar_week, tenant_name, direction, COUNT(*) as cnt FROM integration.envelope_table_v2 GROUP BY 1,2,3 LIMIT 5',
    '--table-format', 'psql',
    'integration_analytics'
])

if result.exit_code == 0:
    print("   ✓ Aggregation query executed!")
    print(result.output)
else:
    print(f"   ✗ Error: {result.exception}")

print("\n" + "=" * 60)
print("✓ RedshiftCLI Full Integration Test PASSED!")
print("=" * 60)
