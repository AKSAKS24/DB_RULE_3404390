from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple
import re
import json

app = FastAPI(
    title="ABAP J_1BBRANCH Replacement for SAP Note 3404390 (Use CDS View P_BusinessPlace)"
)

# Mapping of obsolete table to replacement CDS view
OBSOLETE_TABLE_MAP = {
    "J_1BBRANCH": "P_BusinessPlace"
}

# Regex to capture different ABAP table usage patterns
TABLE_USAGE_RE = re.compile(
    r"""\b(?P<stmt>
            SELECT\b.*?\bFROM\b\s+         # SELECT ... FROM table
            |JOIN\b\s+                     # JOIN table
            |TABLES\b\s+                   # TABLES table
            |(TYPE|LIKE)\b\s+TABLE\s+OF\s+ # TYPE TABLE OF table
            |(TYPE|LIKE)\b\s+              # TYPE table
        )
        (?P<table>\w+)                     # capture table name
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""

def migrate_table_usage(stmt_prefix: str, table: str) -> Optional[str]:
    """
    If table is the obsolete J_1BBRANCH, return a suggested replacement.
    - SELECT ... FROM → suggest SELECT * FROM P_BusinessPlace
    - Otherwise → replace table in original prefix and append TODO.
    If table not obsolete, return None.
    """
    table_up = table.upper()
    if table_up in OBSOLETE_TABLE_MAP:
        new_table = OBSOLETE_TABLE_MAP[table_up]
        todo_comment = (
            f"* TODO: {table_up} is obsolete in S/4HANA (SAP Note 3404390). "
            f"Use released CDS view {new_table} instead. Adjust field mappings accordingly."
        )

        if stmt_prefix.strip().upper().startswith("SELECT"):
            # Always suggest SELECT * FROM new_table for SELECT statements
            return f"SELECT * FROM {new_table}\n{todo_comment}"
        else:
            # For non-SELECT usages (TABLES, TYPE, LIKE, JOIN) keep original prefix but update table
            replaced_stmt = re.sub(rf"\b{table}\b", new_table, stmt_prefix + table, flags=re.IGNORECASE)
            return f"{replaced_stmt}\n{todo_comment}"

    # Table not obsolete
    return None

def find_table_usages(code: str):
    """
    Scan ABAP code and find any table usages matching our patterns.
    """
    out = []
    for m in TABLE_USAGE_RE.finditer(code):
        out.append({
            "stmt_prefix": m.group("stmt"),
            "table": m.group("table"),
            "span": m.span(0),
            "stmt_type": "TABLE_USAGE"
        })
    return out

def apply_span_replacements(src: str, repls: List[Tuple[Tuple[int, int], str]]) -> str:
    """
    Apply replacements from last to first to avoid disrupting indexes.
    """
    out = src
    for (s, e), r in sorted(repls, key=lambda x: x[0][0], reverse=True):
        out = out[:s] + r + out[e:]
    return out

@app.post("/remediate-array")
def remediate_array(units: List[Unit]):
    results = []
    for u in units:
        src = u.code or ""
        usages = find_table_usages(src)
        replacements = []
        selects_metadata = []

        for usage in usages:
            table = usage["table"]
            stmt_prefix = usage["stmt_prefix"]

            suggested_stmt = migrate_table_usage(stmt_prefix, table)

            if suggested_stmt:
                # Only add to selects if suggestion exists (obsolete table)
                sel_info = {
                    "table": table,
                    "target_type": None,
                    "target_name": None,
                    "start_char_in_unit": usage["span"][0],
                    "end_char_in_unit": usage["span"][1],
                    "used_fields": [],
                    "ambiguous": False,
                    "suggested_fields": None,
                    "suggested_statement": suggested_stmt
                }
                replacements.append((usage["span"], suggested_stmt))
                selects_metadata.append(sel_info)

        apply_span_replacements(src, replacements)

        obj = json.loads(u.model_dump_json())
        obj["selects"] = selects_metadata  # Only obsolete ones now
        results.append(obj)

    return results