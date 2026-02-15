import pandas as pd
import json
from pathlib import Path

# ---- Config ----
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw" / "discharge_summaries"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Path to the MIMIC-IV notes file
NOTES_PATH = RAW_DIR / "discharge.csv.gz"

# Limit for Day 1 (keep it small!)
MAX_DOCS = 50

# ---- Load notes ----
print("Loading notes...")
notes_df = pd.read_csv(
    NOTES_PATH,
    compression="gzip",
    low_memory=False
)
print("Total rows:", len(notes_df))
print("Unique admissions:", notes_df["hadm_id"].nunique())
print("Unique patients:", notes_df["subject_id"].nunique())

# ---- Filter discharge summaries ----
discharge_df = (
    notes_df.sort_values("note_seq")
      .groupby("hadm_id")
      .tail(1)
      .head(50)
)

print(f"Found {len(discharge_df)} discharge summaries")

# ---- Write one JSON file per document ----
for _, row in discharge_df.iterrows():
    doc = {
        "document_id": row["note_id"],
        "subject_id": row["subject_id"],
        "hadm_id": row["hadm_id"],
        "note_type": row["note_type"],
        "note_seq": row["note_seq"],
        "charttime": str(row["charttime"]),
        "storetime": str(row["storetime"]),
        "text": row["text"]
    }

    out_path = RAW_DIR / f"{row['note_id']}.json"
    with open(out_path, "w") as f:
        json.dump(doc, f, indent=2)

print("Done. Discharge summaries written to:", RAW_DIR)
