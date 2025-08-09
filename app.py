"""
Universal PDF → Excel (editable tables) for Relay
-------------------------------------------------
- Nahraješ JAKÉKOLI PDF s tabulkami (výkazy, rozvahy, VZZ, atd.).
- Vrátí XLSX, kde je každá nalezená tabulka v editovatelných buňkách.
- Na stránce ponechá JEDNU (největší) tabulku, aby nevznikaly duplikáty.
- Funguje bez OCR (textová PDF). OCR můžeme doplnit později přes Docker.

Endpointy:
- POST /pdf_to_struct_xlsx            … nahraný PDF soubor (multipart)
- POST /pdf_from_url_to_struct_xlsx   … URL na PDF (server si PDF stáhne sám)
"""

import io, re, os, unicodedata
from typing import List, Tuple
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse
import pdfplumber
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import requests  # pro endpoint s URL

app = FastAPI(title="Universal PDF→Excel (tables)")

# -------- helpers --------
def nz(x):
    return "" if x is None else str(x)

def norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip()
    return s

# čísla typu 12 345 nebo (1 234)
NUM_RX = re.compile(r"\(?-?\d+(?:\s\d{3})*\)?")

# -------- extraction (bez OCR) --------
def extract_tables_pdfplumber(pdf_bytes: bytes, min_cols: int) -> List[pd.DataFrame]:
    """
    Z každé PDF stránky vrátí max 1 tabulku (největší nalezenou).
    Fallback z textu spustí jen tehdy, když se nepodaří detekovat "skutečnou" tabulku.
    """
    tables: List[pd.DataFrame] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            page_tables: List[pd.DataFrame] = []

            # 1) explicitní tabulky
            raw_tbls = p.extract_tables() or []
            for t in raw_tbls:
                rows = [[nz(c) for c in (trow or [])] for trow in t or []]
                if not rows:
                    continue
                width = max(len(r) for r in rows)
                rows = [r + [""] * (width - len(r)) for r in rows]
                df = pd.DataFrame(rows)
                if df.shape[1] >= min_cols and df.shape[0] >= 2:
                    page_tables.append(df)

            # 2) fallback z textu POUZE pokud nic nenašli
            if not page_tables:
                txt = p.extract_text() or ""
                if txt:
                    lines = [l for l in txt.splitlines() if l.strip()]
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
                        label = re.sub(
                            r"\s+\(?-?\d+(?:\s\d{3})*\)?(?:\s+\(?-?\d+(?:\s\d{3})*\)?)*\s*$",
                            "", ln
                        ).strip()
                        rec.append([label] + values)
                    if rec:
                        width = max(len(r) for r in rec)
                        rec = [r + [""] * (width - len(r)) for r in rec]
                        df = pd.DataFrame(rec)
                        if df.shape[1] >= min_cols and df.shape[0] >= 2:
                            page_tables.append(df)

            # 3) ze stránky vyber největší tabulku
            if page_tables:
                biggest = max(page_tables, key=lambda d: d.shape[0] * d.shape[1])
                tables.append(biggest)

    return tables

# -------- API basics --------
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/healthz")
def health():
    return {"ok": True}

# -------- API: upload souboru (multipart) --------
@app.post("/pdf_to_struct_xlsx")
async def pdf_to_struct_xlsx(
    file: UploadFile = File(..., description="PDF s tabulkami (výkazy atd.)"),
    min_cols: int = Form(2),
    max_sheets: int = Form(20),
    include_log: bool = Form(True),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Nahraj PDF soubor.")

    pdf_bytes = await file.read()

    try:
        tables = extract_tables_pdfplumber(pdf_bytes, min_cols)
    except Exception as e:
        raise HTTPException(500, f"Chyba při parsování PDF: {e}")

    if not tables:
        raise HTTPException(422, "V dokumentu jsem nenašel žádné tabulky.")

    wb = Workbook()
    wb.remove(wb.active)

    count = 0
    for idx, df in enumerate(tables, start=1):
        if count >= max_sheets:
            break
        df0 = df.fillna("")
        header = [f"col{j+1}" for j in range(df0.shape[1])]
        df0.columns = header
        first = df0.iloc[0].tolist()
        if any(re.search(r"[A-Za-z]", nz(x)) for x in first) and not all(NUM_RX.fullmatch(nz(x) or "") for x in first[1:]):
            df0.columns = [norm_text(nz(x)) or f"col{j+1}" for j, x in enumerate(first)]
            df0 = df0.iloc[1:]

        ws = wb.create_sheet(f"Tab {idx}")
        for r in dataframe_to_rows(df0, index=False, header=True):
            ws.append(r)
        count += 1

    if include_log:
        log = wb.create_sheet("_LOG")
        log.append(["Zdroj PDF", file.filename])
        log.append(["Počet stránek/tabulek", len(tables)])
        log.append(["Pozn.", "Každá stránka → 1 hlavní tabulka (největší)."])

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    base = os.path.splitext(os.path.basename(file.filename))[0]
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={base}_tables.xlsx"}
    )

# -------- API: z URL (Relay-friendly) --------
@app.post("/pdf_from_url_to_struct_xlsx")
async def pdf_from_url_to_struct_xlsx(
    file_url: str = Form(..., description="Veřejná URL na PDF (Drive webContentLink / File URL / přímé PDF)"),
    min_cols: int = Form(2),
    max_sheets: int = Form(20),
    include_log: bool = Form(True),
):
    # 1) normalizace Google Drive URL na přímé stažení
    def normalize_gdrive(u: str) -> str:
        u = u.strip()
        m = re.match(r"https?://drive\.google\.com/file/d/([^/]+)/", u)
        if m:
            return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
        return u

    url = normalize_gdrive(file_url)

    # 2) stáhni PDF
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(400, f"Nepodařilo se stáhnout PDF z URL: {e}")

    ctype = (r.headers.get("Content-Type") or "").lower()
    if "pdf" not in ctype and not r.content.startswith(b"%PDF"):
        raise HTTPException(415, "Stažený obsah nevypadá jako PDF.")

    pdf_bytes = r.content

    # 3) extrakce stejnou funkcí
    try:
        tables = extract_tables_pdfplumber(pdf_bytes, min_cols)
    except Exception as e:
        raise HTTPException(500, f"Chyba při parsování PDF: {e}")

    if not tables:
        raise HTTPException(422, "V dokumentu jsem nenašel žádné tabulky.")

    wb = Workbook()
    wb.remove(wb.active)

    count = 0
    for idx, df in enumerate(tables, start=1):
        if count >= max_sheets:
            break
        df0 = df.fillna("")
        header = [f"col{j+1}" for j in range(df0.shape[1])]
        df0.columns = header
        first = df0.iloc[0].tolist()
        if any(re.search(r"[A-Za-z]", nz(x)) for x in first) and not all(NUM_RX.fullmatch(nz(x) or "") for x in first[1:]):
            df0.columns = [norm_text(nz(x)) or f"col{j+1}" for j, x in enumerate(first)]
            df0 = df0.iloc[1:]

        ws = wb.create_sheet(f"Tab {idx}")
        for rrow in dataframe_to_rows(df0, index=False, header=True):
            ws.append(rrow)
        count += 1

    if include_log:
        log = wb.create_sheet("_LOG")
        log.append(["Zdroj URL", file_url])
        log.append(["Počet stránek/tabulek", len(tables)])
        log.append(["Pozn.", "Každá stránka → 1 hlavní tabulka (největší)."])

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=tables.xlsx"}
    )
