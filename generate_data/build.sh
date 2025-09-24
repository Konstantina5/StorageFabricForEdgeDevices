#!/bin/bash

# Folders for data and SQL
DATA_DIR="load_data"
SQL_DIR="sql_files"

tables=("region" "nation" "part" "supplier" "partsupp" "customer" "orders" "lineitem")

for t in "${tables[@]}"; do
    echo "Loading $t..."

    # Create schema from SQL files
    sqlite3 "${t}.db" < "${SQL_DIR}/${t}.sql"

    # Clean trailing pipes and save to a temp file
    CLEAN_FILE="${DATA_DIR}/${t}_clean.tbl"
    sed 's/|$//' "${DATA_DIR}/${t}.tbl" > "$CLEAN_FILE"

    # Import the data into SQLite
    sqlite3 "${t}.db" <<EOF
.mode csv
.separator "|"
.import $CLEAN_FILE $t
EOF

    # Verify row count
    row_count=$(sqlite3 "${t}.db" "SELECT COUNT(*) FROM ${t};")
    echo "Table $t loaded with $row_count rows."

    # Remove the cleaned temp file
    rm -f "$CLEAN_FILE"
done

echo "All tables loaded successfully."
