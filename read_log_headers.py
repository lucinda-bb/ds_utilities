from __future__ import annotations

import csv
import hashlib
from pathlib import Path
from collections import Counter, defaultdict

# ---------- CONFIG ----------
ROOT_FOLDER = r"C:\Deep_Springs_Aquisuite_Database"   # <-- change to your mirror root
PATTERN = "*.log.csv"                                 # your files
OUT_DIR = r"C:\2026 Utilities Management\log_files_headers"

# If you want to ignore columns that vary but aren't "real signals" (optional),
# put their lowercase names here:
IGNORE_COLS = {
    # "error", "lowalarm", "highalarm", "time(utc)"
}

# ---------- HELPERS ----------
def norm_col(col: str) -> str:
    """Normalize a column name for comparison."""
    return " ".join(col.strip().split())

def schema_fingerprint(cols: list[str]) -> str:
    """
    Produce a stable fingerprint for a header format.
    Uses normalized column names and preserves order (order can matter for some exports).
    """
    joined = "\n".join(cols).encode("utf-8", errors="ignore")
    return hashlib.sha1(joined).hexdigest()[:12]

def read_header_fast(path: Path) -> list[str] | None:
    """Read only the first row (header) from a CSV-like file."""
    try:
        with path.open("r", newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return None
            return [norm_col(c) for c in header]
    except Exception:
        # Some files may be locked or have weird encoding; treat as unreadable
        return None

def main() -> None:
    root = Path(ROOT_FOLDER)
    out = Path(OUT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    files = sorted(root.rglob(PATTERN))
    print(f"Scanning: {root}")
    print(f"Found {len(files)} files matching {PATTERN}")

    # Group by header fingerprint
    schema_to_files: dict[str, list[str]] = defaultdict(list)
    schema_to_cols: dict[str, list[str]] = {}
    unreadable: list[str] = []

    # Also collect global column stats
    col_counter = Counter()

    for i, fp in enumerate(files, start=1):
        header = read_header_fast(fp)
        if header is None:
            unreadable.append(str(fp))
            continue

        # Optionally ignore some columns (by lowercase match)
        filtered = [c for c in header if c.lower() not in IGNORE_COLS]

        sig = schema_fingerprint(filtered)

        schema_to_files[sig].append(str(fp))
        # record the first seen header for this schema
        if sig not in schema_to_cols:
            schema_to_cols[sig] = filtered

        # global stats
        for c in filtered:
            col_counter[c] += 1

        if i % 500 == 0:
            print(f"  processed {i}/{len(files)}...")

    # ---------- OUTPUT 1: Summary text ----------
    summary_path = out / "header_formats_summary.txt"
    schemas_sorted = sorted(schema_to_files.items(), key=lambda kv: len(kv[1]), reverse=True)

    with summary_path.open("w", encoding="utf-8") as f:
        f.write(f"Root: {root}\n")
        f.write(f"Pattern: {PATTERN}\n")
        f.write(f"Total files scanned: {len(files)}\n")
        f.write(f"Unreadable files: {len(unreadable)}\n")
        f.write(f"Distinct header formats: {len(schema_to_files)}\n\n")

        for rank, (sig, flist) in enumerate(schemas_sorted, start=1):
            cols = schema_to_cols[sig]
            f.write(f"=== Format #{rank} | fingerprint={sig} | files={len(flist)} | cols={len(cols)} ===\n")
            # show first few files
            for ex in flist[:5]:
                f.write(f"  {ex}\n")
            if len(flist) > 5:
                f.write(f"  ... ({len(flist)-5} more)\n")
            f.write("Columns:\n")
            for c in cols:
                f.write(f"  {c}\n")
            f.write("\n")

        if unreadable:
            f.write("=== Unreadable files (first 50) ===\n")
            for ex in unreadable[:50]:
                f.write(f"  {ex}\n")
            if len(unreadable) > 50:
                f.write(f"  ... ({len(unreadable)-50} more)\n")

    # ---------- OUTPUT 2: CSV of formats ----------
    formats_csv = out / "header_formats.csv"
    with formats_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["format_rank", "fingerprint", "file_count", "col_count", "example_file", "columns_joined"])
        for rank, (sig, flist) in enumerate(schemas_sorted, start=1):
            cols = schema_to_cols[sig]
            w.writerow([rank, sig, len(flist), len(cols), flist[0], " | ".join(cols)])

    # ---------- OUTPUT 3: CSV of column frequency ----------
    cols_csv = out / "column_frequency.csv"
    with cols_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["column_name", "file_count_containing_column_estimate"])
        for col, cnt in col_counter.most_common():
            w.writerow([col, cnt])

    print("\nDone.")
    print(f"Distinct header formats: {len(schema_to_files)}")
    print(f"Unreadable files: {len(unreadable)}")
    print(f"Wrote:\n  {summary_path}\n  {formats_csv}\n  {cols_csv}")

if __name__ == "__main__":
    main()
