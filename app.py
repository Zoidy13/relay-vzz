"""
FastAPI service: Universal PDF → Excel (editable tables) for Relay
-------------------------------------------------------------------
Goal
- You upload ANY financial PDF (annual report pages, Rozvaha, VZZ, mixed).
- The service returns an XLSX where each detected table is on its own sheet (editable cells, not images).
- Works out of the box for PDFs with a text layer. Optional OCR mode for scanned PDFs (needs Docker deploy with system deps).

Endpoints
- POST /pdf_to_struct_xlsx   → best-effort table extraction with pdfplumber; fallback heuristic from text lines
  Form fields: file (PDF)
               min_cols (int, optional; default 2)  – ignore tiny 1-col fragments
               max_sheets (int, optional; default 20)
               include_log (bool, optional; default true)
               ocr (bool, optional; default false)  – if true, requires Tesseract + poppler (Docker variant)

Notes
- In "ocr=false" mode the API will parse text-based PDFs robustly on Render free plan.
- In "ocr=true" mode (for scanned PDFs) you must deploy with Docker that installs tesseract-ocr and poppler-utils.
"""

import io, re, os, unicodedata
from typing import List, Tuple
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
import pdfplumber
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

try:
    # Optional OCR pieces (only used when ocr=True). These imports are light; system deps are needed at runtime.
    import pytesseract
    from pdf2image import convert_from_bytes
    from PIL import Image
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

app = FastAPI(title="Universal PDF→Excel (tables)")

# ----------------- helpers -----------------

def nz(x):
    return "" if x is None else str(x)

def norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Extract numbers like 12 345 or (1 234) etc.
NUM_RX = re.compile(r"\(?-?\d+(?:\s\d{3})*\)?")

# ----------------- extraction -----------------

def extract_tables_pdfplumber(pdf_bytes: bytes, min_cols: int) -> List[pd.DataFrame]:
    tables: List[pd.DataFrame] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            # try explicit tables
            raw_tbls = p.extract_tables() or []
            for t in raw_tbls:
                rows = [[nz(c) for c in (trow or [])] for trow in t or []]
                # simple cleanup: drop empty cols
                if not rows:
                    continue
                # pad rows to same length
                width = max(len(r) for r in rows)
                rows = [r + [""]*(width-len(r)) for r in rows]
                df = pd.DataFrame(rows)
                # discard too-narrow tables
                if df.shape[1] >= min_cols and df.shape[0] >= 2:
                    tables.append(df)
            # fallback: attempt to build 2-col table from lines (label | numbers...)
            txt = p.extract_text() or ""
            if txt:
                lines = [l for l in txt.splitlines() if l.strip()]
                rec = []
                for ln in lines:
                    # split label and trailing numbers
                    nums = list(NUM_RX.finditer(ln))
                    if not nums:
                        continue
                    # take numeric tail starting from last token; keep up to 6 numbers
                    values = []
                    tail = ln
                    for m in reversed(nums):
                        tok = m.group(0)
                        # ensure it's at end or followed by whitespace
                        if m.end() >= len(ln) or ln[m.end()] == " ":
                            values.insert(0, tok)
                            # continue collecting from previous
                        else:
                            break
                    if not values:
                        continue
                    # label = line minus the last contiguous numbers block
                    label = re.sub(r"\s+\(?-?\d+(?:\s\d{3})*\)?(?:\s+\(?-?\d+(?:\s\d{3})*\)?)*\s*$", "", ln).strip()
                    row = [label] + values
                    rec.append(row)
                if rec:
                    width = max(len(r) for r in rec)
                    rec = [r + [""]*(width-len(r)) for r in rec]
                    df = pd.DataFrame(rec)
                    if df.shape[1] >= min_cols and df.shape[0] >= 2:
                        tables.append(df)
    return tables


def extract_tables_ocr(pdf_bytes: bytes, min_cols: int) -> List[pd.DataFrame]:
    if not OCR_AVAILABLE:
        raise HTTPException(500, "OCR mode requested but pytesseract/pdf2image not available (deploy Docker variant).")
    # Render each page as image, OCR to text, then heuristic table build (label | numbers...)
    imgs = convert_from_bytes(pdf_bytes, dpi=300)
    all_tables: List[pd.DataFrame] = []
    for img in imgs:
        text = pytesseract.image_to_string(img, lang="ces+eng")
        lines = [l for l in text.splitlines() if l.strip()]
        rec = []
        for ln in lines:
            nums = list(NUM_RX.finditer(ln))
            if not nums:
                continue
            values = []
            for m in reversed(nums):
                tok = m.group(0)
                if m.end() >= len(ln) or ln[m.end()] == " ":
                    values.insert(0, tok)
                else:
                    break
            if not values:
                continue
            label = re.sub(r"\s+\(?-?\d+(?:\s\d{3})*\)?(?:\s+\(?-?\d+(?:\s\d{3})*\)?)*\s*$", "", ln).strip()
            rec.append([label] + values)
        if rec:
            width = max(len(r) for r in rec)
            rec = [r + [""]*(width-len(r)) for r in rec]
            df = pd.DataFrame(rec)
            if df.shape[1] >= min_cols and df.shape[0] >= 2:
                all_tables.append(df)
    return all_tables

# ----------------- API -----------------

@app.post("/pdf_to_struct_xlsx")
async def pdf_to_struct_xlsx(
    file: UploadFile = File(..., description="PDF s tabulkami (výkazy atd.)"),
    min_cols: int = Form(2),
    max_sheets: int = Form(20),
    include_log: bool = Form(True),
    ocr: bool = Form(False),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Nahraj PDF soubor.")

    pdf_bytes = await file.read()

    # 1) Extract tables
    try:
        if ocr:
            tables = extract_tables_ocr(pdf_bytes, min_cols)
        else:
            tables = extract_tables_pdfplumber(pdf_bytes, min_cols)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Chyba při parsování PDF: {e}")

    if not tables:
        raise HTTPException(422, "V dokumentu jsem nenašel žádné tabulky.")

    # 2) Build XLSX
    wb = Workbook()
    # remove default sheet; we'll create per-table
    wb.remove(wb.active)

    count = 0
    for idx, df in enumerate(tables, start=1):
        if count >= max_sheets:
            break
        # tidy: set first row as header if looks like header (contains any alphabetic label and at least one year/number)
        df0 = df.copy()
        df0 = df0.fillna("")
        # normalize header
        header = [f"col{j+1}" for j in range(df0.shape[1])]
        df0.columns = header
        # try to promote first row to header if it seems like column names (contains non-numeric words)
        first = df0.iloc[0].tolist()
        if any(re.search(r"[A-Za-z]", nz(x)) for x in first) and not all(NUM_RX.fullmatch(nz(x) or "") for x in first[1:]):
            df0.columns = [norm_text(nz(x)) or f"col{j+1}" for j, x in enumerate(first)]
            df0 = df0.iloc[1:]
        title = df0.columns[0][:28] if df0.columns.size > 0 else f"Tabulka {idx}"
        ws = wb.create_sheet(f"Tab {idx}")
        for r in dataframe_to_rows(df0, index=False, header=True):
            ws.append(r)
        count += 1

    if include_log:
        log = wb.create_sheet("_LOG")
        log.append(["Zdroj PDF", file.filename])
        log.append(["Počet tabulek", len(tables)])
        log.append(["Použit OCR", str(bool(ocr))])
        log.append(["Pozn.", "Listy pojmenované 'Tab 1..N' obsahují extrahované tabulky."])

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    base = os.path.splitext(os.path.basename(file.filename))[0]
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={base}_tables.xlsx"}
    )

# ----------------- requirements (reference) -----------------
# pdfplumber
# pandas
# openpyxl
# fastapi
# uvicorn[standard]
# python-multipart
# Optional OCR mode (Docker deploy):
# pytesseract
# pdf2image
# pillow

# ----------------- Dockerfile (optional for OCR mode) -----------------
# If you need OCR on scanned PDFs, use this Dockerfile and deploy on Render with Docker.
# Save as Dockerfile at repo root and choose Docker deploy.
# ----------------- requirements (reference) -----------------
# pdfplumber
# pandas
# openpyxl
# fastapi
# uvicorn[standard]
# python-multipart
# Optional OCR mode (Docker deploy):
# pytesseract
# pdf2image
# pillow

# ----------------- Dockerfile (optional for OCR mode) -----------------
# Uložte do samostatného souboru "Dockerfile" v rootu repozitáře, ne do app.py
# Příklad:
# FROM python:3.11-slim
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     tesseract-ocr tesseract-ocr-ces poppler-utils \
#     && rm -rf /var/lib/apt/lists/*
# WORKDIR /app
# COPY requirements.txt /app/requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt
# COPY . /app
# ENV PORT=10000
# CMD ["uvicorn","app:app","--host","0.0.0.0","--port","10000"]

