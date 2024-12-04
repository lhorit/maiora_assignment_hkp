# maiora_assignment_hkp

# PySpark Sales Data Pipeline

## Overview
This project processes sales data from two regions, transforms it based on business rules, and stores it in an SQLite database.

### Features
- Combines data from two regions.
- Adds calculated fields like `total_sales` and `net_sale`.
- Ensures data quality by removing duplicates and filtering invalid entries.
- The transformed data is stored in `db.sqlite3` with the table name `sales_data`.

### How to Run
1. Install dependencies:
pip install -r requirements.txt