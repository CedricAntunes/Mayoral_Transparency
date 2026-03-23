# Author: Cedric Antunes (FGV-CEPESP) ------------------------------------------
# Date: March, 2026 ------------------------------------------------------------

# ------------------------------------------------------------------------------
# Required packages ------------------------------------------------------------
# ------------------------------------------------------------------------------
import sys, subprocess

def ensure_package(pkg_name, import_name=None):
    import_name = import_name or pkg_name
    try:
        __import__(import_name)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg_name])

ensure_package("pymupdf", "fitz")
ensure_package("openpyxl")
ensure_package("pyarrow")
ensure_package("tqdm")

# ------------------------------------------------------------------------------
# Imports ----------------------------------------------------------------------
# ------------------------------------------------------------------------------
import os
import re
import glob
import unicodedata
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import fitz  # PyMuPDF
import pandas as pd
import numpy as np
from tqdm.auto import tqdm

# ------------------------------------------------------------------------------
# Mounting Google Drive --------------------------------------------------------
# ------------------------------------------------------------------------------
from google.colab import drive
drive.mount('/content/drive', force_remount=False)

# ------------------------------------------------------------------------------
# BASIC CONFIGURATIONS & PATHS -------------------------------------------------
# ------------------------------------------------------------------------------
# Folder containing ONLY the RJ 2012 PDFs
PDF_ROOT = "/content/drive/MyDrive/Mayoral_Pledges_2012-2024/2012_RJ/RJ"

# Metadata file
METADATA_PATH = "/content/drive/MyDrive/final_clean_data.csv"

# Output folder
OUTPUT_DIR = "/content/drive/MyDrive/pledge_outputs_rj_2012"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Required columns in metadata
FILE_COL   = "PDF"   
GENDER_COL = "DESCRICAO_GENERO"     

# Candidate ID
ID_COL = "ID_CEPESP"              

# Year and state columns
YEAR_COL   = "ANO_ELEICAO"          
YEAR_VALUE = 2012

STATE_COL   = "UF"         
STATE_VALUE = "RJ"

# Optional extra columns to keep if they exist
EXTRA_KEEP_COLS = [
    # "NM_CANDIDATO",
    # "NM_URNA_CANDIDATO",
    # "ANO_ELEICAO",
    # "SG_UF",
    # "CD_MUNICIPIO",
    # "NM_MUNICIPIO",
    # "NR_TURNO",
    # "SG_PARTIDO",
]

# Low worker count is usually more stable on Drive
NUM_WORKERS = 2

# If True, saves PDF extraction log and text table
SAVE_EXTRACTION_TABLE = True

# If you want to test on a tiny subset first, set an integer like 50
MAX_MATCHED_PDFS = None

# ------------------------------------------------------------------------------
# Helper functions -------------------------------------------------------------
# ------------------------------------------------------------------------------
def strip_accents(text):
    if pd.isna(text):
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))

def normalize_spaces(text):
    return re.sub(r"\s+", " ", str(text)).strip()

def clean_text_basic(text):
    if pd.isna(text):
        return ""
    text = str(text).replace("\x00", " ")
    text = normalize_spaces(text)
    return text

def normalize_for_matching(text):
    text = clean_text_basic(text).lower()
    text = strip_accents(text)
    text = normalize_spaces(text)
    return text

def standardize_file_key(x):
    if pd.isna(x):
        return np.nan
    x = os.path.basename(str(x).strip())
    x = x.lower()
    if not x.endswith(".pdf"):
        x = x + ".pdf"
    return x

def load_metadata(path):
    ext = Path(path).suffix.lower()

    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)

    elif ext == ".csv":
        encodings_to_try = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
        seps_to_try = [",", ";"]

        last_error = None
        for enc in encodings_to_try:
            for sep in seps_to_try:
                try:
                    return pd.read_csv(
                        path,
                        encoding=enc,
                        sep=sep,
                        on_bad_lines="skip",
                        low_memory=False
                    )
                except Exception as e:
                    last_error = e

        raise ValueError(f"Could not read CSV file {path}. Last error: {last_error}")

    else:
        raise ValueError(f"Unsupported metadata file type: {ext}")

def extract_text_from_pdf(pdf_path):
    with fitz.open(pdf_path) as doc:
        return "\n".join(page.get_text("text") for page in doc)

def extract_pdf_record(args):
    file_key, pdf_path = args
    try:
        raw_text = extract_text_from_pdf(pdf_path)
        clean_text = clean_text_basic(raw_text)
        normalized_text = normalize_for_matching(clean_text)

        try:
            with fitz.open(pdf_path) as doc:
                page_count = len(doc)
        except Exception:
            page_count = np.nan

        return {
            "file_key": file_key,
            "pdf_path": pdf_path,
            "extract_status": "ok",
            "extract_error": "",
            "page_count": page_count,
            "raw_text": raw_text,
            "clean_text": clean_text,
            "text_normalized": normalized_text,
        }
    except Exception as e:
        return {
            "file_key": file_key,
            "pdf_path": pdf_path,
            "extract_status": "error",
            "extract_error": str(e),
            "page_count": np.nan,
            "raw_text": "",
            "clean_text": "",
            "text_normalized": "",
        }

def count_words(text):
    if not text:
        return 0
    return len(re.findall(r"\b\w+\b", text, flags=re.UNICODE))

def count_sentences(text):
    if not text:
        return 0
    sents = re.split(r"[.!?]+", text)
    sents = [s.strip() for s in sents if s.strip()]
    return len(sents)

def avg_sentence_length(text):
    nw = count_words(text)
    ns = count_sentences(text)
    return np.nan if ns == 0 else nw / ns

# ------------------------------------------------------------------------------
# Theme dictionary -------------------------------------------------------------
# ------------------------------------------------------------------------------
THEME_PATTERNS = {
    "transparency": r"""
        \b(
            transparencia\w*|
            transparente\w*|
            acesso\ a\ informacao|
            dados\ abertos|
            ouvidoria\w*|
            prestacao\ de\ contas|
            controle\ social
        )\b
    """,
    "corruption": r"""
        \b(
            corrup\w*|
            fraud\w*|
            propina\w*|
            improbidade\w*|
            desvio\w*|
            lavagem\ de\ dinheiro
        )\b
    """,
    "participation": r"""
        \b(
            particip\w*|
            audiencia\w*\ publica\w*|
            conselho\w*|
            orcamento\ participativo|
            escuta\ popular|
            dialogo\ com\ a\ populacao|
            controle\ popular
        )\b
    """,
    "health": r"""
        \b(
            saude\w*|
            hospital\w*|
            upa\w*|
            ubs\w*|
            posto\w*\ de\ saude|
            medico\w*|
            medica\w*|
            enfermeir\w*|
            remedi\w*|
            farmacia\w*|
            atencao\ basica
        )\b
    """,
    "education": r"""
        \b(
            educa\w*|
            escola\w*|
            professor\w*|
            professora\w*|
            aluno\w*|
            aluna\w*|
            alfabetiza\w*|
            ensino\w*|
            merenda\ escolar
        )\b
    """,
    "women": r"""
        \b(
            mulher\w*|
            feminina\w*|
            genero|
            violencia\ domestica|
            feminicidio|
            empreendedorismo\ feminino|
            saude\ da\ mulher
        )\b
    """,
    "family": r"""
        \b(
            familia\w*|
            assistencia\ social|
            vulnerab\w*|
            protecao\ social|
            cadastro\ unico|
            bolsa\ familia
        )\b
    """,
    "childcare": r"""
        \b(
            creche\w*|
            pre\ escola\w*|
            pre-escola\w*|
            educacao\ infantil|
            bercario\w*
        )\b
    """,
    "security": r"""
        \b(
            seguranca\w*|
            guarda\ municipal|
            violencia\w*|
            crime\w*|
            criminalidade|
            policiamento|
            monitoramento
        )\b
    """,
    "management": r"""
        \b(
            gesta\w*|
            eficiencia\w*|
            moderniza\w*|
            planejamento\w*|
            meta\w*|
            indicador\w*|
            governanca\w*|
            digitaliza\w*|
            inovacao\w*|
            resultado\w*
        )\b
    """,
    "infrastructure": r"""
        \b(
            infraestrutura\w*|
            pavimenta\w*|
            asfalto\w*|
            saneamento\w*|
            esgoto\w*|
            agua\w*|
            iluminacao\w*|
            habitacao\w*|
            moradia\w*|
            obra\w*|
            mobilidade\w*|
            transporte\w*
        )\b
    """,
    "jobs_economy": r"""
        \b(
            emprego\w*|
            renda\w*|
            trabalho\w*|
            empreendedor\w*|
            economia\w*|
            comercio\w*|
            industria\w*|
            desenvolvimento\ economico
        )\b
    """,
}

COMPILED_PATTERNS = {
    theme: re.compile(pattern, flags=re.IGNORECASE | re.VERBOSE)
    for theme, pattern in THEME_PATTERNS.items()
}
THEMES = list(COMPILED_PATTERNS.keys())

def compute_theme_features(text_normalized):
    n_words = count_words(text_normalized)
    out = {
        "n_chars": len(text_normalized) if text_normalized else 0,
        "n_words": n_words,
        "n_sentences": count_sentences(text_normalized),
        "avg_sentence_length": avg_sentence_length(text_normalized),
    }
    for theme, pattern in COMPILED_PATTERNS.items():
        matches = pattern.findall(text_normalized) if text_normalized else []
        count = len(matches)
        per_1000 = (count / n_words * 1000) if n_words > 0 else 0
        binary = int(count > 0)
        out[f"{theme}_count"] = count
        out[f"{theme}_per_1000"] = per_1000
        out[f"{theme}_binary"] = binary
    return out

# ------------------------------------------------------------------------------
# Load metadata ----------------------------------------------------------------
# ------------------------------------------------------------------------------
print("\nLoading metadata...")
meta = load_metadata(METADATA_PATH).copy()
print("Original metadata shape:", meta.shape)

# Optional filtering by year/state
if YEAR_COL is not None and YEAR_COL in meta.columns:
    meta = meta.loc[meta[YEAR_COL] == YEAR_VALUE].copy()
    print(f"After filtering {YEAR_COL} == {YEAR_VALUE}: {meta.shape}")

if STATE_COL is not None and STATE_COL in meta.columns:
    meta = meta.loc[meta[STATE_COL].astype(str).str.upper() == str(STATE_VALUE).upper()].copy()
    print(f"After filtering {STATE_COL} == {STATE_VALUE}: {meta.shape}")

required_cols = [FILE_COL, GENDER_COL]
missing = [c for c in required_cols if c not in meta.columns]
if missing:
    raise ValueError(f"Missing required columns in metadata: {missing}")

keep_cols = required_cols + [c for c in EXTRA_KEEP_COLS if c in meta.columns]
if ID_COL is not None and ID_COL in meta.columns and ID_COL not in keep_cols:
    keep_cols = [ID_COL] + keep_cols

meta = meta[keep_cols].copy()
meta["file_key"] = meta[FILE_COL].apply(standardize_file_key)

if ID_COL is None or ID_COL not in meta.columns:
    meta.insert(0, "candidate_row_id", range(1, len(meta) + 1))
    FINAL_ID_COL = "candidate_row_id"
else:
    FINAL_ID_COL = ID_COL

# Saving original filtered metadata before deduplication
meta.to_csv(os.path.join(OUTPUT_DIR, "filtered_metadata_before_dedup.csv"), index=False)

# Deduplicate by pledge file so runoff duplicates do not count twice
meta_unique = meta.drop_duplicates(subset=["file_key"]).copy()

print("Filtered metadata shape:", meta.shape)
print("Unique-pledge metadata shape:", meta_unique.shape)
print("Rows dropped by deduplicating repeated file_key:", len(meta) - len(meta_unique))

# ------------------------------------------------------------------------------
# Index PDFs in RJ 2012 folder only --------------------------------------------
# ------------------------------------------------------------------------------
print("\nIndexing PDFs in RJ 2012 folder...")
pdf_files = glob.glob(os.path.join(PDF_ROOT, "**", "*.pdf"), recursive=True)
pdf_files += glob.glob(os.path.join(PDF_ROOT, "**", "*.PDF"), recursive=True)

pdf_map = {}
duplicate_pdf_keys = []

for path in pdf_files:
    key = standardize_file_key(os.path.basename(path))
    if key in pdf_map:
        duplicate_pdf_keys.append({
            "file_key": key,
            "pdf_path": path,
            "existing_path": pdf_map[key]
        })
    else:
        pdf_map[key] = path

pdf_index = pd.DataFrame({
    "file_key": list(pdf_map.keys()),
    "pdf_path": list(pdf_map.values())
})

print("Unique PDFs indexed in RJ folder:", len(pdf_index))
print("Duplicate basenames found in folder:", len(duplicate_pdf_keys))

if duplicate_pdf_keys:
    pd.DataFrame(duplicate_pdf_keys).to_csv(
        os.path.join(OUTPUT_DIR, "duplicate_pdf_keys.csv"),
        index=False
    )

# Keeping only PDFs that appear in filtered/deduplicated metadata
needed_keys = set(meta_unique["file_key"].dropna().unique())
pdf_index_matched = pdf_index[pdf_index["file_key"].isin(needed_keys)].copy()

if MAX_MATCHED_PDFS is not None:
    pdf_index_matched = pdf_index_matched.head(MAX_MATCHED_PDFS).copy()

print("Unique file keys in filtered unique metadata:", len(needed_keys))
print("Matched PDFs to extract:", len(pdf_index_matched))

# Saving file keys in metadata not found in folder
pdf_keys_available = set(pdf_index["file_key"])
unmatched_keys = sorted(list(needed_keys - pdf_keys_available))
if unmatched_keys:
    pd.DataFrame({"file_key": unmatched_keys}).to_csv(
        os.path.join(OUTPUT_DIR, "metadata_file_keys_not_found_in_pdf_folder.csv"),
        index=False
    )
    print("Metadata file keys not found in RJ folder:", len(unmatched_keys))

# ------------------------------------------------------------------------------
# Extracting text only from matched PDFs ---------------------------------------
# ------------------------------------------------------------------------------
print("\nExtracting text from matched PDFs only...")
items = list(zip(pdf_index_matched["file_key"], pdf_index_matched["pdf_path"]))

records = []
with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    for rec in tqdm(executor.map(extract_pdf_record, items), total=len(items), desc="Extracting PDFs"):
        records.append(rec)

text_df = pd.DataFrame(records)

print("\nExtraction status:")
print(text_df["extract_status"].value_counts(dropna=False))

if SAVE_EXTRACTION_TABLE:
    text_df.to_parquet(os.path.join(OUTPUT_DIR, "pdf_text_extraction.parquet"), index=False)
    text_df[["file_key", "pdf_path", "extract_status", "extract_error", "page_count"]].to_csv(
        os.path.join(OUTPUT_DIR, "pdf_extraction_log.csv"),
        index=False
    )

# ------------------------------------------------------------------------------
# Merging metadata + extracted text --------------------------------------------
# ------------------------------------------------------------------------------
print("\nMerging unique metadata with extracted text...")
master = meta_unique.merge(
    text_df,
    on="file_key",
    how="left",
    validate="1:1"
)

master["text_found"] = master["raw_text"].fillna("").str.len().gt(0).astype(int)

print("Master shape:", master.shape)
print("Rows with text found:", int(master["text_found"].sum()))
print("Rows without matching text:", int((master["text_found"] == 0).sum()))

unmatched = master.loc[
    master["text_found"] == 0,
    [c for c in master.columns if c in [FINAL_ID_COL, FILE_COL, "file_key", GENDER_COL]]
]
if len(unmatched) > 0:
    unmatched.to_csv(os.path.join(OUTPUT_DIR, "unmatched_metadata_rows.csv"), index=False)

# ------------------------------------------------------------------------------
# Computing candidate-level text features --------------------------------------
# ------------------------------------------------------------------------------
print("\nComputing candidate-level text features...")
feature_rows = []
for txt in tqdm(master["text_normalized"].fillna(""), total=len(master), desc="Computing features"):
    feature_rows.append(compute_theme_features(txt))

feature_df = pd.DataFrame(feature_rows)
master = pd.concat([master.reset_index(drop=True), feature_df.reset_index(drop=True)], axis=1)

# ------------------------------------------------------------------------------
# Creating long theme dataset --------------------------------------------------
# ------------------------------------------------------------------------------
print("\nCreating long theme dataset...")
base_cols = [FINAL_ID_COL, FILE_COL, "file_key", GENDER_COL]
base_cols = [c for c in base_cols if c in master.columns]

long_parts = []
for theme in THEMES:
    tmp = master[base_cols].copy()
    tmp["theme"] = theme
    tmp["count"] = master[f"{theme}_count"]
    tmp["per_1000_words"] = master[f"{theme}_per_1000"]
    tmp["mentioned"] = master[f"{theme}_binary"]
    long_parts.append(tmp)

theme_long = pd.concat(long_parts, ignore_index=True)

# ------------------------------------------------------------------------------
# Summary tables by gender -----------------------------------------------------
# ------------------------------------------------------------------------------
print("\nCreating descriptive summaries by gender...")
theme_rate_cols = [f"{t}_per_1000" for t in THEMES]
theme_bin_cols  = [f"{t}_binary" for t in THEMES]

summary_means = master.groupby(GENDER_COL, dropna=False)[theme_rate_cols].mean().reset_index()
summary_mentions = master.groupby(GENDER_COL, dropna=False)[theme_bin_cols].mean().reset_index()

summary_means.to_csv(os.path.join(OUTPUT_DIR, "summary_theme_rates_by_gender.csv"), index=False)
summary_mentions.to_csv(os.path.join(OUTPUT_DIR, "summary_theme_mentions_by_gender.csv"), index=False)

# ------------------------------------------------------------------------------
# Saving final outputs ---------------------------------------------------------
# ------------------------------------------------------------------------------
print("\nSaving outputs...")
master_csv = os.path.join(OUTPUT_DIR, "candidate_pledge_master_rj_2012.csv")
master_parquet = os.path.join(OUTPUT_DIR, "candidate_pledge_master_rj_2012.parquet")
theme_long_csv = os.path.join(OUTPUT_DIR, "candidate_theme_long_rj_2012.csv")
theme_long_parquet = os.path.join(OUTPUT_DIR, "candidate_theme_long_rj_2012.parquet")

master.to_csv(master_csv, index=False)
master.to_parquet(master_parquet, index=False)
theme_long.to_csv(theme_long_csv, index=False)
theme_long.to_parquet(theme_long_parquet, index=False)

# ------------------------------------------------------------------------------
# Quick diagnostics ------------------------------------------------------------
# ------------------------------------------------------------------------------
print("\nDone.")
print("\nSaved files:")
print(master_csv)
print(master_parquet)
print(theme_long_csv)
print(theme_long_parquet)
print(os.path.join(OUTPUT_DIR, "summary_theme_rates_by_gender.csv"))
print(os.path.join(OUTPUT_DIR, "summary_theme_mentions_by_gender.csv"))

print("\nPreview of master data:")
display(master.head())

print("\nMean theme frequency per 1,000 words by gender:")
display(summary_means)

print("\nShare of candidates mentioning each theme by gender:")
display(summary_mentions)
