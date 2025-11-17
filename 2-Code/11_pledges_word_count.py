# Author: Cedric Antunes (FGV-CEPESP)
# Date: November 2025 

# Basic configuration
ROOT_HINT = "Mayoral_Pledges"           # part of the folder name to search for
USE_OCR   = False                       # True if many PDFs are scanned images
OCR_LANGS = "por+eng"                   # tesseract languages
OCR_THRESHOLD = 20                      # try next extractor if words < threshold
OUTPUT_NAME  = "mayoral_wordcounts.csv" # results (append-only)
ERRORS_NAME  = "mayoral_wordcounts_errors.csv"
MANIFEST_NAME = "_pdf_manifest.csv"     # list of PDFs with stable ids
BATCH_SAVE_EVERY = 200                  # flush results every N files

# Setting Drive ----------------------------------------------------------------
from google.colab import drive
drive.mount('/content/drive', force_remount=True)

!pip -q install pypdf pdfminer.six pandas tqdm PyMuPDF

if USE_OCR:
    !apt-get -y -qq install tesseract-ocr tesseract-ocr-por tesseract-ocr-eng poppler-utils
    !pip -q install pdf2image pytesseract pillow

# Locating folder --------------------------------------------------------------
from pathlib import Path
roots_to_scan = [Path("/content/drive/MyDrive"), Path("/content/drive/Shareddrives")]
candidates = []
for root in roots_to_scan:
    if root.exists():
        for p in root.rglob("*"):
            try:
                if p.is_dir() and ROOT_HINT.lower() in p.name.lower():
                    candidates.append(p)
            except Exception:
                pass

if not candidates:
    raise SystemExit(f'No folder containing "{ROOT_HINT}" found under MyDrive/Shareddrives.')

candidates = sorted(candidates, key=lambda p: len(str(p)), reverse=True)
INPUT_DIR = candidates[0]
print("Using root folder:", INPUT_DIR)

# Helper functions -------------------------------------------------------------
import os, re, pandas as pd
from typing import List, Tuple
from tqdm import tqdm

WORD_REGEX = re.compile(r"\b[\w'-]+\b", flags=re.UNICODE)

def clean_text(s: str) -> str:
    s = s.replace("\r", "")
    s = re.sub(r"-\n", "", s)   # join hyphenated line breaks
    s = re.sub(r"\n+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def count_words(s: str) -> int:
    return len(WORD_REGEX.findall(s)) if s else 0

def extract_text_pymupdf(path: str) -> str:
    try:
        import fitz
        doc = fitz.open(path)
        return "\n".join(page.get_text("text") or "" for page in doc)
    except Exception:
        return ""

def extract_text_pypdf(path: str) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(path)
        return "\n".join((p.extract_text() or "") for p in reader.pages)
    except Exception:
        return ""

def extract_text_pdfminer(path: str) -> str:
    try:
        from pdfminer.high_level import extract_text
        return extract_text(path) or ""
    except Exception:
        return ""

def extract_text_ocr(path: str, langs: str = "eng") -> str:
    if not USE_OCR:
        return ""
    try:
        import pytesseract
        from pdf2image import convert_from_path
        images = convert_from_path(path, dpi=300)
        return "\n".join(pytesseract.image_to_string(img, lang=langs) or "" for img in images)
    except Exception:
        return ""

def process_pdf(abs_path: str) -> int:
    txt = clean_text(extract_text_pymupdf(abs_path))
    wc  = count_words(txt)

    if wc < OCR_THRESHOLD:
        txt2 = clean_text(extract_text_pypdf(abs_path))
        wc2  = count_words(txt2)
        if wc2 > wc: wc = wc2

    if wc < OCR_THRESHOLD:
        txt3 = clean_text(extract_text_pdfminer(abs_path))
        wc3  = count_words(txt3)
        if wc3 > wc: wc = wc3

    if wc < OCR_THRESHOLD and USE_OCR:
        txt4 = clean_text(extract_text_ocr(abs_path, langs=OCR_LANGS))
        wc4  = count_words(txt4)
        if wc4 > wc: wc = wc4

    return wc

def find_pdfs(root: Path) -> List[Path]:
    return sorted(root.rglob("*.pdf"))

# Building ---------------------------------------------------------------------
manifest_path = INPUT_DIR / MANIFEST_NAME
if manifest_path.exists():
    manifest = pd.read_csv(manifest_path)
else:
    pdf_paths = find_pdfs(INPUT_DIR)
    if len(pdf_paths) == 0:
        raise SystemExit("No PDFs detected. Check that the folder is in MyDrive (not only 'Shared with me').")
    rels = [os.path.relpath(str(p), str(INPUT_DIR)) for p in pdf_paths]
    manifest = pd.DataFrame({"file_id": range(len(rels)), "file_name": rels})
    manifest.to_csv(manifest_path, index=False, encoding="utf-8")
print(f"Total PDFs in manifest: {len(manifest)}")

# Reporting progress -----------------------------------------------------------
results_path = INPUT_DIR / OUTPUT_NAME
errors_path  = INPUT_DIR / ERRORS_NAME

done_ids = set()
if results_path.exists():
    # Only read the id column to be fast
    try:
        done_ids = set(pd.read_csv(results_path, usecols=["file_id"])["file_id"])
    except Exception:
        prev = pd.read_csv(results_path, usecols=["file_name"])
        done_ids = set(manifest.loc[manifest["file_name"].isin(prev["file_name"]), "file_id"])

todo = manifest[~manifest["file_id"].isin(done_ids)].copy()
print(f"Already done: {len(done_ids)} | Remaining: {len(todo)}")

# Processing remaining files (if session expires) ------------------------------
rows, errs = [], []
append_results = results_path.exists()
append_errors  = errors_path.exists()

for _, r in tqdm(todo.iterrows(), total=len(todo), desc="Processing PDFs"):
    fid, rel = int(r.file_id), r.file_name
    abs_path = str(INPUT_DIR / rel)
    try:
        wc = process_pdf(abs_path)
        rows.append({"file_id": fid, "file_name": rel, "word_count": wc})
    except Exception as e:
        errs.append({"file_id": fid, "file_name": rel, "error": str(e)})

    # Flushing periodically
    if len(rows) >= BATCH_SAVE_EVERY:
        pd.DataFrame(rows).to_csv(results_path, mode="a", header=not append_results,
                                  index=False, encoding="utf-8")
        append_results = True
        rows.clear()
    if len(errs) >= max(1, BATCH_SAVE_EVERY // 5):
        pd.DataFrame(errs).to_csv(errors_path, mode="a", header=not append_errors,
                                  index=False, encoding="utf-8")
        append_errors = True
        errs.clear()

# Final flush
if rows:
    pd.DataFrame(rows).to_csv(results_path, mode="a", header=not append_results,
                              index=False, encoding="utf-8")
if errs:
    pd.DataFrame(errs).to_csv(errors_path, mode="a", header=not append_errors,
                              index=False, encoding="utf-8")

print("\nDone.")
print("Results:", results_path)
print("Errors :", errors_path if errors_path.exists() else "none")
try:
    display(pd.read_csv(results_path).tail())
except Exception:
    pass
