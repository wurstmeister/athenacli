#!/bin/bash
# Test interactive redshiftcli

# Send commands via stdin
cat <<'EOF' | redshiftcli \
  -h raptor-redshift-cluster.projectraptor.io \
  -U thombeck \
  --aws-profile raptor_engineer \
  integration_analytics
SELECT COUNT(*) as total_rows FROM integration.envelope_table_v2;
SELECT tenant_name, COUNT(*) as cnt FROM integration.envelope_table_v2 GROUP BY 1 ORDER BY 2 DESC LIMIT 3;
\q
EOF
