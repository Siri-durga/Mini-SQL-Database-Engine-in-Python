# parser.py
import re

COMPARISON_OPS = ['>=', '<=', '!=', '=', '>', '<']

class SQLParseError(Exception):
    pass

def normalize_whitespace(s: str) -> str:
    return re.sub(r'\s+', ' ', s).strip()

def parse(sql: str) -> dict:
    """
    Mini SQL parser:
    SELECT <cols_or_count> FROM <table> [WHERE <col> <op> <value>];
    """
    if not sql or not sql.strip():
        raise SQLParseError("Empty SQL")

    # Remove trailing semicolon and normalize spaces
    s = normalize_whitespace(sql.strip().rstrip(';'))

    # Must start with SELECT
    if not s.upper().startswith("SELECT "):
        raise SQLParseError("Query must start with SELECT")

    # Ensure FROM is present
    if " FROM " not in s.upper():
        raise SQLParseError("Missing FROM clause")

    # ----- SPLIT INTO SELECT and REST -----
    raw_select_part, raw_rest = s.split(" from ", 1) if " from " in s else s.split(" FROM ", 1)
    select_body = raw_select_part[len("SELECT "):].strip()

    # ----- SPLIT REST INTO TABLE and WHERE -----
    raw_rest_upper = raw_rest.upper()
    if " WHERE " in raw_rest_upper:
        # WHERE exists
        parts = re.split(r"\sWHERE\s", raw_rest, flags=re.IGNORECASE)
        from_part = parts[0].strip()
        where_body = parts[1].strip()
    else:
        from_part = raw_rest.strip()
        where_body = None

    # final table name (lowercase)
    table_name = from_part.lower()

    # ----------------------------
    # PARSE SELECT SECTION
    # ----------------------------
    select = {"type": None, "cols": None, "count_col": None}

    # COUNT(*)
    if select_body.upper().startswith("COUNT("):
        m = re.match(r"COUNT\(\s*(\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)$",
                     select_body,
                     re.IGNORECASE)
        if not m:
            raise SQLParseError("Invalid COUNT() syntax")

        select["type"] = "count"
        select["count_col"] = m.group(1).lower()
        select["cols"] = None

    else:
        # SELECT * or columns
        if select_body == "*":
            select["type"] = "cols"
            select["cols"] = ["*"]
        else:
            cols = [c.strip().lower() for c in select_body.split(",") if c.strip()]
            if not cols:
                raise SQLParseError("No columns specified in SELECT")

            # validate columns
            for c in cols:
                if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", c):
                    raise SQLParseError(f"Invalid column name: {c}")

            select["type"] = "cols"
            select["cols"] = cols

    # ----------------------------
    # PARSE WHERE CLAUSE
    # ----------------------------
    where = None
    if where_body:
        op = None
        for candidate in COMPARISON_OPS:
            if candidate in where_body:
                op = candidate
                break

        if op is None:
            raise SQLParseError("WHERE must contain a comparison operator")

        left, right = where_body.split(op, 1)

        col = left.strip().lower()
        raw_val = right.strip()

        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", col):
            raise SQLParseError(f"Invalid column in WHERE: {col}")

        # Parse value
        if raw_val.startswith("'") and raw_val.endswith("'"):
            val = raw_val[1:-1]
            val_type = "str"
        else:
            # Try number
            try:
                if '.' in raw_val:
                    val = float(raw_val)
                else:
                    val = int(raw_val)
                val_type = "num"
            except ValueError:
                # Try bare word string
                if re.match(r"^[A-Za-z0-9_]+$", raw_val):
                    val = raw_val
                    val_type = "str"
                else:
                    raise SQLParseError("WHERE value must be number or string")

        where = {"col": col, "op": op, "val": val, "val_type": val_type}

    # FINAL PARSED DICT
    return {
        "select": select,
        "from": table_name,
        "where": where
    }
