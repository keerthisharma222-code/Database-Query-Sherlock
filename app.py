import os
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import PromptTemplate

# --- Config ---
DATA_DIR = os.getenv("DATA_DIR", "data")
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-pro")
API_KEY = os.getenv("GOOGLE_API_KEY")
ROW_LIMIT_DEFAULT = int(os.getenv("ROW_LIMIT_DEFAULT", "200"))

# Initialize Gemini LLM
llm = ChatGoogleGenerativeAI(model=MODEL_NAME, google_api_key=API_KEY)

# Load CSVs
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

tables = {}
for file in os.listdir(DATA_DIR):
    if file.endswith(".csv"):
        table_name = file.replace(".csv", "")
        tables[table_name] = pd.read_csv(os.path.join(DATA_DIR, file))

app = FastAPI(title="Database Copilot", version="1.0")

@app.get("/health")
def health():
    if tables:
        return {"status": "ok", "tables_loaded": list(tables.keys())}
    return JSONResponse(status_code=500, content={"status": "error", "detail": "No CSV files found"})

@app.get("/schema")
def schema():
    schema_info = []
    for name, df in tables.items():
        schema_info.append({
            "table": name,
            "columns": [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
        })
    return {"tables": schema_info}

@app.get("/query")
def query(table: str, filter_column: Optional[str] = None, filter_value: Optional[str] = None, limit: int = ROW_LIMIT_DEFAULT):
    if table not in tables:
        raise HTTPException(status_code=400, detail=f"Table '{table}' not found")

    df = tables[table]
    if filter_column and filter_value:
        if filter_column not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{filter_column}' not found in table '{table}'")
        df = df[df[filter_column].astype(str) == filter_value]

    return {"rows": df.head(limit).to_dict(orient="records"), "row_count": len(df)}

@app.post("/nl-query")
def nl_query(prompt: str = Query(...)):
    # Convert NL to SQL using Gemini
    template = PromptTemplate(
        input_variables=["question"],
        template="Convert this natural language question into an SQL query for SQLite: {question}"
    )
    sql_query = llm.predict(template.format(question=prompt))
    return {"generated_sql": sql_query}

@app.get("/report")
def report(table: str, format: str = "csv"):
    if table not in tables:
        raise HTTPException(status_code=400, detail=f"Table '{table}' not found")

    file_name = f"{table}_report.{format}"
    file_path = os.path.join(DATA_DIR, file_name)

    if format == "csv":
        tables[table].to_csv(file_path, index=False)
    elif format == "excel":
        tables[table].to_excel(file_path, index=False)
    else:
        raise HTTPException(status_code=400, detail="Unsupported format. Use 'csv' or 'excel'.")

    return {"file_link": f"/download/{file_name}"}

@app.get("/download/{file_name}")
def download(file_name: str):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return JSONResponse(content={"message": f"Download {file_name} from {file_path}"})
