import os
import re
import json
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Body, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# --- Config ---
DB_URL = os.getenv("DB_URL", "sqlite:///sample.db")  # Start with SQLite
API_KEY = os.getenv("API_KEY")  # Optional: set to enable x-api-key auth
ROW_LIMIT_DEFAULT = int(os.getenv("ROW_LIMIT_DEFAULT", "200"))

# --- App ---
app = FastAPI(title="SQL Gateway", version="1.0")

def get_engine() -> Engine:
    # pool_pre_ping helps avoid stale connections in cloud DBs
    return create_engine(DB_URL, pool_pre_ping=True)

engine = get_engine()

def is_safe_select(sql: str) -> bool:
    """Very simple guardrail: allow only read-only statements."""
    s = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.S | re.M).strip().lower()
    # Disallow multiple statements
    if ";" in s.strip().rstrip(";"):
        return False
    # Must start with WITH/SELECT/EXPLAIN/SHOW/DESCRIBE/PRAGMA (sqlite)
    allowed_starts = ("with", "select", "explain", "show", "describe", "pragma")
    if not s.startswith(allowed_starts):
        return False
    # Ban common DDL/DML keywords
    banned = [
        r"\binsert\b", r"\bupdate\b", r"\bdelete\b", r"\bdrop\b",
        r"\btruncate\b", r"\balter\b", r"\bcreate\b(?!\s+temp)",  # allow temp if you like
        r"\breplace\b", r"\bgrant\b", r"\brevoke\b", r"\battach\b", r"\bvacuum\b"
    ]
    return not any(re.search(pat, s) for pat in banned)

def enforce_limit(sql: str) -> str:
    s = sql.strip()
    # If already has LIMIT/TOP, leave as-is
    if re.search(r"\blimit\s+\d+\b", s, flags=re.I) or re.search(r"\btop\s+\d+\b", s, flags=re.I):
        return s
    # Generic LIMIT wrapper works for SQLite/Postgres/MySQL
    return f"SELECT * FROM ({s}) AS _sub LIMIT {ROW_LIMIT_DEFAULT}"

def require_api_key(header_key: Optional[str]):
    if API_KEY and header_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

class SqlRequest(BaseModel):
    query: str
    params: Optional[Dict[str, Any]] = None

@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})

@app.get("/schema")
def schema(x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    try:
        insp = inspect(engine)
        tables = []
        for tbl in insp.get_table_names():
            cols = insp.get_columns(tbl)
            tables.append({
                "table": tbl,
                "columns": [{"name": c["name"], "type": str(c["type"]), "nullable": c.get("nullable", True)} for c in cols]
            })
        return {"db_url": DB_URL.split("://", 1)[0] + "://...", "tables": tables}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sql")
def run_sql(payload: SqlRequest = Body(...), x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    sql = payload.query
    if not is_safe_select(sql):
        raise HTTPException(status_code=400, detail="Only single-statement, read-only SELECT/EXPLAIN/SHOW queries are allowed.")

    sql_limited = enforce_limit(sql)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_limited), parameters=payload.params or {})
            rows = [dict(r._mapping) for r in result]
            # try include dialect name for downstream decisions if needed
            dialect = conn.dialect.name
        return {"rows": rows, "row_count": len(rows), "limit_applied": sql_limited != sql, "dialect": dialect}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=400, detail=str(e))
