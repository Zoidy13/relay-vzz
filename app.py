def extract_tables_pdfplumber(pdf_bytes: bytes, min_cols: int) -> List[pd.DataFrame]:
    tables: List[pd.DataFrame] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            page_tables: List[pd.DataFrame] = []

            # 1) „Skutečné“ tabulky
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

            # 2) Fallback z textu spustit jen když se nenašla žádná tabulka
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

            # 3) Z každé stránky buď vyber největší tabulku, nebo deduplikuj
            if page_tables:
                # vyber největší podle (řádky × sloupce)
                biggest = max(page_tables, key=lambda d: d.shape[0] * d.shape[1])
                tables.append(biggest)

    return tables
