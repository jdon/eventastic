#!/usr/bin/env bash
set -euo pipefail

# Install postgres if it's missing
if ! command -v psql >/dev/null 2>&1; then
    apt-get update -y
    DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql
fi

# Start the postgres service if not already running
if ! pg_isready -q; then
    service postgresql start
fi

# Ensure the default postgres user has a password so tests can authenticate
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'password';" >/dev/null

echo "Postgres service started on port 5432"
