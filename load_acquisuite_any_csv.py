# this script loads log.csv files found in the local PC folder that stores files
# copied from the file server. It then parses the timestamp, source, metric, value, and unit from each column
# finally, it uploads the data to a postgres database


# import libraries needed for parsing and database connection
# re for regex parsing--> use regular expressions to find or validate patterns in text
# define pattern and apply the pattern to find matches, extract matched parts
import re

# pathlib for file handling--> working with file paths and directories
# work with filesystem paths in an object oriented way by creating Path objects
from pathlib import Path

#pandas for csv parsing--> understand csv files and store in dataframe for manipulation
import pandas as pd

#psycopg2 for postgres connection
import psycopg2
from psycopg2.extras import execute_values

# define folder where csv files are located to be parsed
ROOT_FOLDER = r"C:\Deep_Springs_Aquisuite_Database"

#define postgres connection parameters
PG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ds_utilities",
    "user": "postgres",
    "password": "DSC2026"
}
# define table in postgres to insert log info into
TABLE = "measurements"

# define patterns to be used be regex identify metric types in column names 
# match metric pattern in column name to standard name in database
METRIC_PATTERNS = [
    (r"\bave\s*rate\b$", "avg_rate"),
    (r"\bavg\s*rate\b$", "avg_rate"),
    (r"\brate\b$", "rate"),
    (r"\binstantaneous\b$", "instantaneous"),
    (r"\bdemand\b$", "demand"),
    (r"\bave\b$", "avg"),
    (r"\bavg\b$", "avg"),
    (r"\baverage\b$", "avg"),
    (r"\bmin\b$", "min"),
    (r"\bmax\b$", "max"),
    (r"\btotal\b$", "total"),
    (r"\bcount\b$", "count"),
    (r"\bvalue\b$", "value"),
    (r"\blevel\b$", "level"),
    (r"\bstatus\b$", "status"),
]

# regex to extract unit from column name, looking for last (...) group at end of string
UNIT_RE = re.compile(r"\(([^()]*)\)\s*$")  # last (...) group at end

#  function to infer quantity type (pressure, flow, power, energy, pulse) from column name and unit
def infer_quantity(source: str, unit: str | None) -> str | None:
    s = source.lower()
    u = (unit or "").lower()

    # explicit words in the name
    if "pressure" in s:
        return "pressure"
    if "flow" in s:
        return "flow"
    if "power" in s:
        return "power"
    if "energy" in s:
        return "energy"
    if s.startswith("pulse #") or "pulse" in s:
        return "pulse"

    # infer from units
    if u == "kw":
        return "power"
    if u == "kwh":
        return "energy"
    if "psi" in u:
        return "pressure"
    if "gpm" in u:
        return "flow"

    return None

# function to parse column name into source, metric, and unit
def parse_column(col: str):
    if col is None:
        return None
    c = col.strip()
    if c == "" or c == "-":
        return None
    cl = c.lower()

    # Extract unit
    unit = None
    m = UNIT_RE.search(c)
    if m:
        unit = m.group(1).strip()
        base = c[:m.start()].strip()
    else:
        base = c
    base_norm = " ".join(base.split())

    # Identify metric
    metric = None
    source = base_norm
    for pat, name in METRIC_PATTERNS:
        if re.search(pat, base_norm, flags=re.IGNORECASE):
            metric = name
            source = re.sub(pat, "", base_norm, flags=re.IGNORECASE).strip()
            break

    # Normalize source inline
    s = source.lower().strip()
    s = ' '.join(s.split())
    corrections = {'pwer': 'power', 'curent': 'current', 'voltge': 'voltage', 'frequncy': 'frequency', 'enrgy': 'energy', 'dmand': 'demand', 'aparant': 'apparent', 'reactve': 'reactive', 'postive': 'positive', 'negtive': 'negative', 'sumation': 'sum', 'avrage': 'average', 'instntaneous': 'instantaneous', 'facotr': 'factor'}
    for wrong, right in corrections.items():
        s = s.replace(wrong, right)
    source = s.title()

    # Default metric
    if metric is None:
        q = infer_quantity(source, unit)
        if q == "energy":
            metric = "energy"
        elif q == "power":
            metric = "power"
        elif q == "flow":
            metric = "flow"
        elif q == "pressure":
            metric = "pressure"
        elif q == "pulse":
            metric = "pulse_total"
        else:
            metric = "value"

    # Prefix quantity
    q = infer_quantity(source, unit)
    if q and metric in ("avg", "min", "max", "rate", "avg_rate", "instantaneous", "demand"):
        metric = f"{q}_{metric}"

    # Extract phase inline
    phase_mapping = {' a': 'A', ' b': 'B', ' c': 'C', ' sum': 'sum', ' total': 'sum', ' ave': 'avg', ' average': 'avg', ' a-b': 'AB', ' b-c': 'BC', ' a-c': 'AC'}
    phase = None
    sl = source.lower()
    for key, val in phase_mapping.items():
        if key in sl:
            phase = val
            break
    if phase:
        metric = f"{metric}_{phase}"

    # Infer system inline
    system_mapping = {'wyman creek': 'wyman_creek', 'reservoir by-pass': 'bypass', 'bypass': 'bypass', 'deep well pump': 'deep_well_pump', 'booster pump': 'booster_pump', 'sce': 'grid', 'net meter': 'grid', 'hydro': 'hydro', 'plant': 'hydro', 'solar': 'solar', 'sca': 'sca'}
    source = 'unknown'
    for key, val in system_mapping.items():
        if key in sl:
            source = val
            break

    # Normalize unit
    if unit:
        unit = unit.replace("per minute", "/min").replace("per hour", "/hr")

    return source, metric, unit

def load_one_csv(path):
    df = pd.read_csv(path)

    # Required timestamp column
    if "time(UTC)" not in df.columns:
        raise ValueError("Missing time(UTC) column")

    # Parse timestamp as UTC
    df["time"] = pd.to_datetime(df["time(UTC)"], errors="coerce", utc=True)
    if df["time"].isna().any():
        bad = df.loc[df["time"].isna(), "time(UTC)"].head(3).tolist()
        raise ValueError(f"Timestamp parse failed. Examples: {bad}")

    # Flags (default to 0 if missing)
    error_val = pd.to_numeric(df.get("error", 0), errors="coerce")
    df["error_flag"] = error_val.fillna(0).astype(bool)
    low_val = pd.to_numeric(df.get("lowalarm", 0), errors="coerce")
    df["low_alarm"] = low_val.fillna(0).astype(bool)
    high_val = pd.to_numeric(df.get("highalarm", 0), errors="coerce")
    df["high_alarm"] = high_val.fillna(0).astype(bool)


    # Pick meter columns: all non-reserved columns (not just those with units)
    reserved = {"time(UTC)", "time", "error", "lowalarm", "highalarm", "error_flag", "low_alarm", "high_alarm"}
    meter_cols = [c for c in df.columns if c not in reserved]

    rows = []
    unparsed = []
    for _, r in df.iterrows():
        for c in meter_cols:
            # Parse: "<source> ... (unit)"
            parsed = parse_column(c)
            if not parsed:
                unparsed.append(c)
                continue

            source, metric, unit = parsed

            val = r[c]
            if pd.isna(val):
                continue

            # Ensure numeric
            try:
                val = float(val)
            except Exception:
                continue

            rows.append((
                r["time"],
                source,
                metric,
                val,
                unit,
                bool(r["error_flag"]),
                bool(r["low_alarm"]),
                bool(r["high_alarm"])
            ))

    
    if not rows:
        return 0
    
    if unparsed:
        print(f"UNPARSED ({len(unparsed)}):", unparsed[:20])
    SOURCE_ALIASES = {
    "Hydo Plant Power": "Hydro Plant",
    "Hydro Plant Power": "Hydro Plant",
    "Solar Array Power": "Solar Array",
    "SCE Main Power Pulse #1": "SCE Main",
    }
    source = SOURCE_ALIASES.get(source, source)
    

    # --- INSERT INTO POSTGRES 
    conn = psycopg2.connect(**PG)
    print("Connecting to:", PG)
    cur = conn.cursor()
    cur.execute("SELECT current_database(), inet_server_port(), current_user;")
    print("DB INFO:", cur.fetchone())
    cur.close()
    try:
        with conn:
            with conn.cursor() as cur:
                sql = (
                        "INSERT INTO measurements "
                        "(time, source, metric, value, unit, error_flag, low_alarm, high_alarm) "
                        "VALUES %s "
                        "ON CONFLICT (time, source, metric) DO NOTHING"
                    )
                print("SQL SENT TO POSTGRES:\n", sql)
                execute_values(cur, sql, rows, page_size=5000)
                print("Inserted rows this run:", cur.rowcount)    
    finally:
            conn.close()

    return len(rows)


def main():
    root = Path(ROOT_FOLDER)
    files = sorted(root.rglob("*.log.csv"))
    print(f"Scanning {root} ... found {len(files)} .log.csv files")

    for i, f in enumerate(files, start=1):
        try:
            n = load_one_csv(f)   # <- capture inserted row count for THIS file
            print(f"[{i}/{len(files)}] OK {f.name}: inserted {n}")
        except Exception as e:
            print(f"[{i}/{len(files)}] FAIL {f.name}: {e}")

if __name__ == "__main__":
    print("Connecting to:", PG)
    main()