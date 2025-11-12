"""
Ingest Excel raw file to Bronze layer.
Usage:
  python3 scripts/ingest_bronze_excel.py --source /path/to/01_Exportaciones_2025_Enero.xlsx
"""
import argparse
from pathlib import Path
import shutil
import hashlib
import json
from datetime import datetime
import re
import pandas as pd

# Map spanish month names to number
MONTH_MAP = {
    'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
    'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
    'septiembre': '09', 'setiembre': '09', 'octubre': '10',
    'noviembre': '11', 'diciembre': '12'
}

# Resolve data_lake root relative to this script so paths are absolute and stable.
REPO_DATA_ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = REPO_DATA_ROOT / "lake" / "bronze"
METADATA_ROOT = REPO_DATA_ROOT / "metadata" / "bronze"

FNAME_PATTERN = re.compile(r'^\d+_Exportaciones_(\d{4})_([A-Za-zÁÉÍÓÚáéíóúÑñ]+)\.xlsx$', re.IGNORECASE)

def compute_checksum(path: Path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def parse_filename(fname: str):
    m = FNAME_PATTERN.match(fname)
    if not m:
        raise ValueError(f"Filename does not match expected pattern: {fname}")
    year = m.group(1)
    month_name = m.group(2).lower()
    if month_name not in MONTH_MAP:
        raise ValueError(f"Unrecognized month name '{month_name}' in filename {fname}")
    month = MONTH_MAP[month_name]
    yyyymm = f"{year}{month}"
    friendly_label = f"{year}-{month}"
    return year, month, yyyymm, friendly_label

def ingest_excel(source_path: str, keep_samples: bool=False, sample_rows:int=50):
    src = Path(source_path)
    if not src.exists():
        raise FileNotFoundError(source_path)

    # parse filename
    year, month, yyyymm, label = parse_filename(src.name)

    dest_dir = BRONZE_ROOT / yyyymm
    dest_dir.mkdir(parents=True, exist_ok=True)

    # copy original file
    dest_file = dest_dir / src.name
    shutil.copy2(src, dest_file)

    checksum = compute_checksum(dest_file)
    size_bytes = dest_file.stat().st_size
    ingest_ts = datetime.utcnow().isoformat() + "Z"

    metadata = {
        "source_name": src.name,
        "source_path": str(src.resolve()),
        "dest_path": str(dest_file.resolve()),
        "checksum_sha256": checksum,
        "size_bytes": size_bytes,
        "ingest_ts": ingest_ts,
        "year": year,
        "month": month,
        "yyyymm": yyyymm,
        "friendly_label": label,
        "sheets": []
    }

    # optional: read sheet info and create small CSV samples for quick preview
    try:
        xls = pd.ExcelFile(dest_file)
        for sheet in xls.sheet_names:
            df = xls.parse(sheet_name=sheet, nrows=sample_rows)
            # If you want just row counts (not read full)
            # read full sheet for count might be expensive; here we read header+sample only
            rows_sample = len(df)
            sample_path = None
            if keep_samples:
                sample_fn = f"sample_{sheet[:30].replace(' ','_')}.csv"
                sample_path = dest_dir / sample_fn
                df.to_csv(sample_path, index=False, encoding='utf-8')
                sample_path = str(sample_path.resolve())
            metadata["sheets"].append({
                "sheet_name": sheet,
                "sample_rows": rows_sample,
                "sample_path": sample_path
            })
    except Exception as e:
        # If Excel cannot be read, still keep metadata; surface the error
        metadata["sheets_error"] = str(e)

    # write metadata file
    meta_dir = METADATA_ROOT / yyyymm
    meta_dir.mkdir(parents=True, exist_ok=True)
    meta_path = meta_dir / f"{src.stem}_metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))

    print(f"Ingested file to bronze: {dest_file}")
    print(f"Metadata written: {meta_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="Path to the Excel file (01_Exportaciones_2025_Enero.xlsx)")
    parser.add_argument("--keep-samples", action="store_true", help="Write small CSV samples of each sheet")
    parser.add_argument("--sample-rows", type=int, default=50, help="Rows to sample for preview CSV")
    args = parser.parse_args()
    ingest_excel(args.source, keep_samples=args.keep_samples, sample_rows=args.sample_rows)
