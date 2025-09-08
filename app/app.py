from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
import re
import json

app = FastAPI(
    title="ABAP J_1BBRANCH Replacement — SAP Note 3404390 (CDS P_BusinessPlace)",
    version="1.0"
)

# --- Obsolete table mapping ---
OBSOLETE_TABLE_MAP = {
    "J_1BBRANCH": "P_BusinessPlace"
}

# --- Models ---
class Finding(BaseModel):
    pgm_name: Optional[str] = None
    inc_name: Optional[str] = None
    type: Optional[str] = None
    name: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    issue_type: Optional[str] = None
    severity: Optional[str] = None
    line: Optional[int] = None
    message: Optional[str] = None
    suggestion: Optional[str] = None
    snippet: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = ""
    start_line: Optional[int] = 0
    end_line: Optional[int] = 0
    code: Optional[str] = ""
    # Our findings output field:
    j1bbranch_findings: Optional[List[Finding]] = None

# --- Regex for ABAP table usage ---
TABLE_USAGE_RE = re.compile(
    r"""\b
        (?P<keyword>
            SELECT\b.*?\bFROM\b\s+       # SELECT ... FROM table
            |JOIN\b\s+                   # JOIN table
            |TABLES\b\s+                 # TABLES table
            |(TYPE|LIKE)\b\s+TABLE\s+OF\s+ # TYPE TABLE OF table
            |(TYPE|LIKE)\b\s+            # TYPE table
        )
        (?P<table>\w+)                   # capture table name
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL
)

def line_of_offset(text: str, off: int) -> int:
    return text.count("\n", 0, off) + 1

def snippet_at(text: str, start: int, end: int) -> str:
    s = max(0, start - 60)
    e = min(len(text), end + 60)
    return text[s:e].replace("\n", "\\n")

def migrate_table_usage(keyword: str, table: str) -> Optional[str]:
    t_up = table.upper()
    if t_up in OBSOLETE_TABLE_MAP:
        new_table = OBSOLETE_TABLE_MAP[t_up]
        comment = (
            f"* TODO: {t_up} is obsolete in S/4HANA (SAP Note 3404390). "
            f"Use released CDS view {new_table} instead. Adjust field mappings accordingly."
        )
        if keyword.strip().upper().startswith("SELECT"):
            return f"SELECT * FROM {new_table}\n{comment}"
        else:
            replaced_stmt = re.sub(rf"\b{table}\b", new_table, keyword + table, flags=re.IGNORECASE)
            return f"{replaced_stmt}\n{comment}"
    return None

def scan_unit(unit: Unit) -> Dict[str, Any]:
    src = unit.code or ""
    findings: List[Dict[str, Any]] = []

    for m in TABLE_USAGE_RE.finditer(src):
        table = m.group("table")
        t_up = table.upper()
        stmt_text = m.group(0)
        repl = migrate_table_usage(m.group("keyword"), table)
        if not repl:
            continue  # Only mark obsolete tables

        finding = {
            "pgm_name": unit.pgm_name,
            "inc_name": unit.inc_name,
            "type": unit.type,
            "name": unit.name,
            "start_line": unit.start_line,
            "end_line": unit.end_line,
            "issue_type": "ObsoleteTableUsage",
            "severity": "warning",
            "line": line_of_offset(src, m.start()),
            "message": (
                f"Obsolete table {table} used. Replace with CDS view {OBSOLETE_TABLE_MAP[t_up]} per SAP Note 3404390."
            ),
            "suggestion": repl,
            "snippet": snippet_at(src, m.start(), m.end()),
            "meta": {
                "original_table": table,
                "replacement_view": OBSOLETE_TABLE_MAP[t_up],
                "original_statement": stmt_text.strip()
            }
        }
        findings.append(finding)
    obj = unit.model_dump()
    obj["j1bbranch_findings"] = findings
    return obj

# --- Main async endpoint ---
@app.post("/remediate-array")
async def scan_j1bbranch(units: List[Unit]):
    # Only return findings if there are any; otherwise don't emit field at all
    results = []
    for u in units:
        res = scan_unit(u)
        # Omit key (negative scenario) if no findings
        if res["j1bbranch_findings"]:
            results.append(res)
        else:
            # No findings? emit nothing (skip this unit) per instructions
            pass
    return results

@app.get("/health")
async def health():
    return {"ok": True}