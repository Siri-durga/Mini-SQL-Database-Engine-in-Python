
# Mini SQL Database Engine (Python)

## Overview
A beginner-friendly, in-memory mini SQL engine that supports:
- Loading CSV files into memory (`list[dict]`)
- SELECT projection (`*` and columns)
- WHERE with a single condition and operators: =, !=, >, <, >=, <=
- COUNT(*) and COUNT(column)
- Interactive CLI (.load, .tables, SQL queries)

## Run
1. Create virtual env and activate:
   - Windows: `python -m venv venv && .\venv\Scripts\Activate.ps1`
   - macOS/Linux: `python3 -m venv venv && source venv/bin/activate`
2. Start REPL: `python cli.py`
3. In REPL:
   - Load a CSV: `.load sample_1.csv`
   - Run SQL: `SELECT * FROM sample_1 WHERE age > 30;`
   - Exit: `exit` or `quit`

## Supported SQL grammar (exact)
SELECT <cols> FROM <table_name> [WHERE <col> <op> <value>];
SELECT COUNT(*) FROM <table_name>;
SELECT COUNT(<column>) FROM <table_name>;

- `<cols>` = `*` or comma-separated column names (identifiers: letters, digits, underscore; must start with a letter/underscore).
- `<op>` = one of `=`, `!=`, `>`, `<`, `>=`, `<=`.
- `<value>` = numeric literal (e.g., 42 or 3.14) OR string in single quotes `'USA'` OR unquoted single token string without spaces (`India`).
- `table_name` is the CSV filename without `.csv` extension (table must be loaded with `.load path/to/<table_name>.csv`).

## Files
- `parser.py` - lightweight SQL parser
- `engine.py` - execution logic and CSV loader
- `cli.py` - REPL
- `sample_1.csv`, `sample_2.csv` - sample datasets


