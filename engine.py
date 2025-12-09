# engine.py
import csv
import os
from typing import List, Dict, Any

from parser import parse, SQLParseError

class TableNotFoundError(Exception):
    pass

class ColumnError(Exception):
    pass

class Engine:
    def __init__(self):
        # map table_name -> rows (list of dict)
        # normalize table_name keys to lowercase
        self.tables: Dict[str, List[Dict[str, Any]]] = {}

    def load_csv(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")
        base = os.path.basename(path)
        name, ext = os.path.splitext(base)
        table_key = name.lower()
        with open(path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [dict(row) for row in reader]
        # normalize: convert numeric-looking strings into numbers for comparisons
        rows = [self._convert_row_types(r) for r in rows]
        # keep the real column names as-is in each row dict
        self.tables[table_key] = rows
        return table_key

    def _convert_row_types(self, row: Dict[str, str]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in row.items():
            # preserve original header name as key
            if v is None:
                out[k] = None
                continue
            val = v.strip()
            if val == '':
                out[k] = None
                continue
            # try int
            try:
                out[k] = int(val)
                continue
            except:
                pass
            # try float
            try:
                out[k] = float(val)
                continue
            except:
                pass
            out[k] = val
        return out

    def _resolve_table(self, table_name: str) -> str:
        # parser returns lowercased table names; be defensive
        key = table_name.lower()
        if key not in self.tables:
            raise TableNotFoundError(f"Table '{table_name}' not loaded. Use .load <path> to load CSV (table name: {table_name})")
        return key

    def _resolve_columns_case_insensitive(self, rows: List[Dict[str, Any]], requested_cols: List[str]) -> List[str]:
        """
        Given rows and a list of requested column names (already lowercased by parser),
        return the real column names as stored in rows (preserve original case).
        """
        if not rows:
            # no rows -> we can't know column names; we allow columns that are requested (will return empty rows)
            return requested_cols

        actual_cols = list(rows[0].keys())
        lower_actual = [c.lower() for c in actual_cols]

        resolved = []
        for rc in requested_cols:
            if rc == '*':
                resolved = actual_cols[:]  # all columns
                break
            if rc.lower() not in lower_actual:
                raise ColumnError(f"Column '{rc}' not found in table")
            resolved.append(actual_cols[lower_actual.index(rc.lower())])
        return resolved

    def execute(self, sql: str):
        parsed = parse(sql)
        table_key = self._resolve_table(parsed['from'])
        rows = self.tables[table_key]

        # apply WHERE filter first (if any)
        if parsed['where']:
            rows = self._apply_where(rows, parsed['where'])

        # handle COUNT aggregate
        if parsed['select']['type'] == 'count':
            count_col = parsed['select']['count_col']
            if count_col == '*':
                return {'cols': ['COUNT(*)'], 'rows': [[len(rows)]]}
            else:
                # count non-null values in that column
                # resolve column name case-insensitively
                if len(rows) == 0:
                    return {'cols': [f"COUNT({count_col})"], 'rows': [[0]]}
                resolved = self._resolve_columns_case_insensitive(rows, [count_col])[0]
                cnt = sum(1 for r in rows if r.get(resolved) is not None)
                return {'cols': [f"COUNT({count_col})"], 'rows': [[cnt]]}

        # projection
        if parsed['select']['cols'] == ['*']:
            # use columns from first row if present
            cols = list(rows[0].keys()) if rows else []
            out_rows = [[r.get(c) for c in cols] for r in rows]
            return {'cols': cols, 'rows': out_rows}
        else:
            req_cols = parsed['select']['cols']
            resolved_cols = self._resolve_columns_case_insensitive(rows, req_cols) if rows else req_cols
            out_rows = [[r.get(c) for c in resolved_cols] for r in rows]
            return {'cols': resolved_cols, 'rows': out_rows}

    def _apply_where(self, rows: List[Dict[str, Any]], where: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        where: { 'col': <lowercase colname>, 'op': str, 'val': val, 'val_type': 'num'|'str' }
        """
        if not rows:
            return []

        requested_col = where['col']
        op = where['op']
        val = where['val']
        val_type = where['val_type']

        # resolve requested_col to real column name
        actual_cols = list(rows[0].keys())
        lower_actual = [c.lower() for c in actual_cols]
        if requested_col.lower() not in lower_actual:
            raise ColumnError(f"Column '{requested_col}' not found in table")
        real_col = actual_cols[lower_actual.index(requested_col.lower())]

        def compare(left, op, right):
            # handle None: always false in comparisons (except maybe = None, but we treat None as NULL)
            if left is None:
                return False
            try:
                if op == '=':
                    return left == right
                if op == '!=':
                    return left != right
                if op == '>':
                    return left > right
                if op == '<':
                    return left < right
                if op == '>=':
                    return left >= right
                if op == '<=':
                    return left <= right
            except Exception:
                return False
            return False

        filtered = []
        for r in rows:
            cell = r.get(real_col)
            if cell is None:
                continue  # treat as not matching

            # prepare left/right based on types
            if val_type == 'num':
                # try to coerce cell to number
                try:
                    left_val = float(cell) if isinstance(cell, str) and '.' in cell else int(cell) if isinstance(cell, (str, int)) and str(cell).isdigit() or isinstance(cell, int) else float(cell)
                except Exception:
                    # unable to coerce -> no match
                    continue
                right_val = float(val)
            else:
                left_val = str(cell)
                right_val = str(val)

            if compare(left_val, op, right_val):
                filtered.append(r)
        return filtered
