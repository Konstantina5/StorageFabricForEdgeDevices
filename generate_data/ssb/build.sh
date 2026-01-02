#!/bin/bash

# Folders for data and SQL
DATA_DIR="load_data"
SQL_DIR="sql_files"

# SSB tables
tables=("customer" "supplier" "part" "date_table" "lineorder")

for t in "${tables[@]}"; do
    echo "Loading SSB table: $t..."

    DB_FILE="${t}.db"
    SQL_FILE="${SQL_DIR}/${t}.sql"
    DATA_FILE="${DATA_DIR}/${t}.tbl"
    CLEAN_FILE="${DATA_DIR}/${t}_clean.tbl"

    if [[ ! -f "$SQL_FILE" ]]; then
        echo "ERROR: Missing schema file $SQL_FILE"
        exit 1
    fi

    if [[ ! -f "$DATA_FILE" ]]; then
        echo "ERROR: Missing data file $DATA_FILE"
        exit 1
    fi

    # Create schema
    sqlite3 "$DB_FILE" < "$SQL_FILE"

    # Clean trailing pipes
    sed 's/|$//' "$DATA_FILE" > "$CLEAN_FILE"

    # Import data
    sqlite3 "$DB_FILE" <<EOF
.mode csv
.separator "|"
.import $CLEAN_FILE $t
EOF

    # Verify row count
    ROWS=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM $t;")
    echo "✔ $t loaded with $ROWS rows"

    # Cleanup
    rm -f "$CLEAN_FILE"
done

echo "✅ All SSB tables loaded successfully."
