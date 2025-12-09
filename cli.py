# cli.py
import sys
import traceback
from engine import Engine, TableNotFoundError, ColumnError
from parser import SQLParseError

PROMPT = "mini-sql> "

def print_result(result):
    cols = result['cols']
    rows = result['rows']
    if not cols:
        print("(no rows / no columns)")
        return
    # nice simple table
    col_widths = [len(str(c)) for c in cols]
    for r in rows:
        for i, cell in enumerate(r):
            col_widths[i] = max(col_widths[i], len(str(cell)) if cell is not None else 4)
    sep = '+'.join('-' * (w + 2) for w in col_widths)
    # header
    header = " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(cols))
    print(header)
    print('-' * len(header))
    for r in rows:
        line = " | ".join((str(cell) if cell is not None else 'NULL').ljust(col_widths[i]) for i, cell in enumerate(r))
        print(line)

def repl():
    eng = Engine()
    print("Mini SQL Engine (type .help for commands).")
    print("Load CSV: .load path/to/file.csv  — the table name becomes the filename without .csv")
    print("Run SQL: e.g. SELECT * FROM sample_1 WHERE age > 30;")
    print("Exit: .exit or quit or exit")
    while True:
        try:
            s = input(PROMPT).strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            break
        if not s:
            continue
        if s.lower() in ('exit', 'quit', '.exit'):
            print("Bye.")
            break
        if s.startswith('.'):
            # meta commands
            parts = s.split(None, 1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else None
            if cmd == '.load':
                if not arg:
                    print("Usage: .load path/to/file.csv")
                    continue
                try:
                    name = eng.load_csv(arg)
                    print(f"Loaded '{arg}' as table '{name}' (rows: {len(eng.tables.get(name, []))})")
                except Exception as e:
                    print(f"Error loading CSV: {e}")
                    # traceback.print_exc()
                continue
            elif cmd == '.tables':
                if eng.tables:
                    for t, rows in eng.tables.items():
                        print(f"{t} (rows={len(rows)})")
                else:
                    print("(no tables loaded)")
                continue
            elif cmd == '.help':
                print(".load path/to/file.csv — load CSV")
                print(".tables — list loaded tables")
                print("exit, quit — leave")
                continue
            else:
                print("Unknown command. Type .help")
                continue
        # otherwise treat as SQL
        try:
            result = eng.execute(s)
            print_result(result)
        except SQLParseError as e:
            print(f"SQL parse error: {e}")
        except TableNotFoundError as e:
            print(f"Table error: {e}")
        except ColumnError as e:
            print(f"Column error: {e}")
        except Exception as e:
            print("Unexpected error:", e)
            traceback.print_exc()

if __name__ == "__main__":
    repl()
