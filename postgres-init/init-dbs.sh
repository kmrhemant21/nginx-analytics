#!/bin/bash
set -e

# Function to create multiple databases
create_multiple_dbs() {
    local database
    for database in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        echo "Creating database: $database"
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
            CREATE DATABASE $database;
            GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
    done
}

# Create databases
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    create_multiple_dbs
fi