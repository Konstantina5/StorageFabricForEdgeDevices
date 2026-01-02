#!/bin/bash

# Folders for data and SQL
DATA_DIR="load_data"
SQL_DIR="sql_files"
DB_NAME="ssb.db"

tables=("customer" "date_table" "part" "supplier" "lineorder")

# Remove old database if it exists
rm -f "$DB_NAME"

for t in "${tables[@]}"; do
    echo "Loading $t into $DB_NAME..."

    # Step 1: Create schema
    sqlite3 "$DB_NAME" < "${SQL_DIR}/${t}.sql"

    # Step 2: Clean trailing pipes and save to a temp file
    CLEAN_FILE="${DATA_DIR}/${t}_clean.tbl"
    sed 's/|$//' "${DATA_DIR}/${t}.tbl" > "$CLEAN_FILE"

    # Step 3: Import data into the database
    sqlite3 "$DB_NAME" <<EOF
.mode csv
.separator "|"
.import $CLEAN_FILE $t
EOF

    # Step 4: Verify row count
    row_count=$(sqlite3 "$DB_NAME" "SELECT COUNT(*) FROM ${t};")
    echo "Table $t loaded with $row_count rows."

    # Step 5: Remove temp cleaned file
    rm -f "$CLEAN_FILE"
done

echo "All tables loaded successfully into $DB_NAME."
