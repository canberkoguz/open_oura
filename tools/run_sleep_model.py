#!/usr/bin/env python3
"""Run Oura's decrypted SleepNet (moonstone) model on our stored ring data to
extract a per-30s hypnogram (DEEP/LIGHT/REM/WAKE).

Inputs from the SQLite event log: IBI (0x60), motion_seconds (0x47), temp (0x46),
bedtime (0x76). SpO2 passed empty (we only have R-ratio, not %). Time axis is the
device-relative deciseconds anchored to the latest event's captured_unix.

Usage: python tools/run_sleep_model.py [--start-ds N --end-ds N] [DB] [--tz H]
                                       [--json]
       (no window → uses the bedtime_period in the DB)
       legacy positional form START_DS END_DS [DB] [TZ] still works
"""
import argparse, sys, json, math, sqlite3, datetime
from pathlib import Path
import torch

from _common import emit_json, fail_no_data, resolve_db

REPO = Path(__file__).resolve().parent.parent
TZ = 1
MODEL = str(REPO / "notes" / "models" / "sleepnet_moonstone_1_2_0.pt")
STAGE = {1: "DEEP", 2: "LIGHT", 3: "REM", 4: "WAKE"}

# The legacy positional form `START_DS END_DS [DB] [TZ]` is peeled off before
# argparse sees it: argparse would bind the first number to `db` and the pair
# would never be recognised as a window.
raw = sys.argv[1:]
legacy_window = legacy_db = legacy_tz = None
if len(raw) >= 2 and raw[0].isdigit() and raw[1].isdigit():
    legacy_window = (int(raw[0]), int(raw[1]))
    tail = raw[2:]
    legacy_db = tail[0] if tail else None
    legacy_tz = float(tail[1]) if len(tail) > 1 else None
    raw = []

p = argparse.ArgumentParser()
p.add_argument("db", nargs="?", default=None)
p.add_argument("--start-ds", type=int, default=None)
p.add_argument("--end-ds", type=int, default=None)
p.add_argument("--tz", type=float, default=1.0)
p.add_argument("--json", action="store_true")
args = p.parse_args(raw)
if legacy_window is not None:
    args.start_ds, args.end_ds = legacy_window
    args.db = legacy_db
    if legacy_tz is not None:
        args.tz = legacy_tz

start_ds, end_ds = args.start_ds, args.end_ds
TZ = args.tz
AS_JSON = args.json
DB = resolve_db(args.db, REPO)


def note(*parts, **kwargs):
    """Progress chatter. Goes to stderr under --json so stdout stays parseable."""
    print(*parts, file=sys.stderr if AS_JSON else sys.stdout, **kwargs)

con = sqlite3.connect(str(DB))
rows = con.execute("SELECT ring_timestamp, tag, decoded_json, captured_unix FROM events "
                   "WHERE decoded_json IS NOT NULL ORDER BY ring_timestamp").fetchall()
max_ds, anchor_unix = max(((r[0], r[3]) for r in rows), key=lambda x: x[0])
def ms(ds):  # device deciseconds -> absolute epoch ms (int64), consistent across signals
    return int(anchor_unix * 1000 - (max_ds - ds) * 100)

if start_ds is None:  # default: most recent bedtime_period in the DB (matches run_models.last_bedtime)
    bt = con.execute("SELECT decoded_json FROM events WHERE tag=118 ORDER BY ring_timestamp DESC").fetchone()
    if bt is None:
        fail_no_data(
            "no_bedtime",
            "no bedtime_period (tag 0x76) in the database, and no explicit "
            "window was given")
    v = json.loads(bt[0])
    start_ds, end_ds = v["bedtime_start_ds"], v["bedtime_end_ds"]

lo, hi = start_ds - 6000, end_ds + 6000  # ±10 min margin
beats, acm, temp = [], [], []
for ds, tag, js, _ in rows:
    if not (lo <= ds <= hi):
        continue
    v = json.loads(js)
    if tag in (0x60, 0x80) and v.get("ibi_ms"):  # ibi_and_amplitude + green_ibi_quality
        ibi = v["ibi_ms"]; amp = v.get("amplitude", [0] * len(ibi))
        t = ms(ds); acc = 0
        for i, x in enumerate(ibi):
            if x <= 0:  # zero/negative IBI can't advance the beat clock — skip (matches run_bdi)
                continue
            acc += x
            valid = 1 if 300 <= x <= 2000 else 0
            beats.append((t + acc, float(x), float(amp[i] if i < len(amp) else 0), valid))
    elif tag == 0x47 and v.get("motion_seconds") is not None:
        acm.append((ms(ds), float(v["motion_seconds"])))
    elif tag == 0x46 and v.get("temps_c"):
        temp.append((ms(ds), float(v["temps_c"][0])))

beats.sort(); acm.sort(); temp.sort()
note(f"window ds [{start_ds}..{end_ds}] ({(end_ds-start_ds)/10/3600:.1f}h)  "
     f"beats={len(beats)} acm={len(acm)} temp={len(temp)}")
if not beats or not any(b[3] == 1 for b in beats):
    fail_no_data("no_ibi", "no valid IBI (tag 0x60/0x80) in this sleep window")

def col(seq, i):
    return [r[i] for r in seq]

def _finite(x):
    """A rounded number, or None where the model had nothing to say."""
    return round(x, 3) if math.isfinite(x) else None

ibi_ts = torch.tensor(col(beats, 0), dtype=torch.int64)
ibi_val = torch.tensor([[b[1], b[2], b[3]] for b in beats], dtype=torch.float32)
acm_ts = torch.tensor(col(acm, 0), dtype=torch.int64)
acm_val = torch.tensor([[a[1]] for a in acm], dtype=torch.float32)
temp_ts = torch.tensor(col(temp, 0), dtype=torch.int64)
temp_val = torch.tensor([[t[1]] for t in temp], dtype=torch.float32)
bedtime = torch.tensor([ms(start_ds), ms(end_ds)], dtype=torch.int64)
spo2_val = torch.empty(0, 1, dtype=torch.float32)
spo2_ts = torch.empty(0, dtype=torch.int64)
scalars = torch.tensor([35, 25, 0, 0, 0], dtype=torch.float32)
tst = torch.tensor([300.0], dtype=torch.float32)

m = torch.jit.load(MODEL, map_location="cpu").eval()
with torch.no_grad():
    ts, staging, apnea, spo2_out, metrics, debug = m(
        bedtime, ibi_val, ibi_ts, acm_val, acm_ts, temp_val, temp_ts,
        spo2_val, spo2_ts, scalars, tst)

stages = [int(s) for s in staging[:, 0].tolist()]
n = len(stages)
if n == 0:
    fail_no_data(
        "no_epochs",
        "the sleep model returned zero epochs for this window — it is too "
        "short or too sparse to stage")
mins = {k: stages.count(c) * 0.5 for c, k in STAGE.items()}
asleep = n * 0.5 - mins["WAKE"]
in_bed = n * 0.5

if AS_JSON:
    emit_json({
        "model": "sleepnet_moonstone_1_2_0",
        "window_ds": [start_ds, end_ds],
        "epochs": n,
        "epoch_sec": 30,
        "in_bed_min": round(in_bed),
        "asleep_min": round(asleep),
        "efficiency_pct": round(100 * asleep / in_bed) if in_bed else 0,
        "stages": {
            name: {"min": round(minutes),
                   "pct": round(100 * minutes / in_bed) if in_bed else 0}
            for name, minutes in mins.items()
        },
        # Per-epoch stage ids, the model's own: 1=DEEP 2=LIGHT 3=REM 4=WAKE.
        "hypnogram": stages,
        # The model returns more than a staging, and this runner used to drop
        # it on the floor. Two unlabelled vectors come back beside the
        # hypnogram; `run_models.py`'s bdi branch already emits its own pair
        # the same way. Passed through verbatim and deliberately unnamed — the
        # field meanings are not recovered, and a guessed name in the payload
        # would be read downstream as a fact. Non-finite entries become null:
        # several are NaN when SpO2 is not supplied, and JSON has no NaN.
        "output_metrics": [_finite(x) for x in metrics.flatten().tolist()],
        "debug_metrics": [_finite(x) for x in debug.flatten().tolist()],
        # How sure the model was of each epoch's stage. Columns 1-4 of the
        # staging tensor are the per-class probabilities — their argmax is the
        # stage in column 0, on every epoch — so the winning one is the
        # model's own confidence in the band it drew. Only the maximum is
        # kept: all four columns would be four times the payload, for three
        # numbers that are recoverable from the fourth only in aggregate and
        # that nothing reads today.
        "stage_confidence": [
            round(p, 3) for p in staging[:, 1:].max(1).values.tolist()],
    })
    raise SystemExit(0)

print(f"\nHypnogram: {n} epochs = {n*0.5:.0f} min in bed")
for k in ["DEEP", "LIGHT", "REM", "WAKE"]:
    pct = 100 * mins[k] / (n * 0.5) if n else 0
    print(f"  {k:<6} {mins[k]:>6.0f} min  ({pct:4.0f}%)")
print(f"  asleep {asleep:.0f} min,  sleep efficiency {100*asleep/(n*0.5):.0f}%")

# compact timeline: one glyph per ~10 min (20 epochs), majority stage
g = {1: "D", 2: "L", 3: "R", 4: "W"}
def hm(ms_):
    return datetime.datetime.utcfromtimestamp(ms_/1000 + TZ*3600).strftime("%H:%M")
print(f"\n  {hm(int(ts[0]))} ", end="")
for i in range(0, n, 20):
    blk = stages[i:i+20]
    maj = max(set(blk), key=blk.count)
    print(g.get(maj, "?"), end="")
print(f" {hm(int(ts[-1]))}   (D=deep L=light R=rem W=wake, ~10min/char)")
