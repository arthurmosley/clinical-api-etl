# generate_measurements.py
import csv, random, argparse
from datetime import timezone, datetime
from faker import Faker

STUDIES = {
    "STUDY001": {
        "types": ["glucose", "cholesterol", "weight", "height"],
        "units": {"glucose":"mg/dL","cholesterol":"mg/dL","weight":"kg","height":"cm"},
    },
    "STUDY002": {
        "types": ["blood_pressure", "heart_rate"],
        "units": {"blood_pressure":"mmHg","heart_rate":"bpm"},
    },
}

SITES = ["SITE_A","SITE_B","SITE_C","SITE_D","SITE_E"]

def gen_value(study, m, rnd):
    if study == "STUDY001":
        if m == "glucose":     return round(rnd.uniform(70, 140), 1)
        if m == "cholesterol": return round(rnd.uniform(120, 240), 0)
        if m == "weight":      return round(rnd.uniform(45, 120), 1)
        if m == "height":      return round(rnd.uniform(150, 200), 0)
    else:
        if m == "blood_pressure":
            s = rnd.randint(95,145); d = rnd.randint(60,95)
            return f"{s}/{d}"
        if m == "heart_rate":
            return int(round(rnd.uniform(55,110), 0))

def write_file(path, faker, rnd, studies, participants_per_study, days_per_participant):
    header = ["study_id","participant_id","measurement_type","value","unit","timestamp","site_id","quality_score"]
    rows = 0
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for study_id, spec in studies.items():
            mtypes = spec["types"]; units = spec["units"]
            for p in range(1, participants_per_study + 1):
                pid = f"P{p:04d}"
                site = rnd.choice(SITES)
                for _ in range(days_per_participant):
                    start_dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                    end_dt   = datetime(2025, 11, 14, 23, 59, 59, tzinfo=timezone.utc)
                    ts = faker.date_time_between(start_date=start_dt, end_date=end_dt, tzinfo=timezone.utc) \
                            .strftime("%Y-%m-%dT%H:%M:%SZ")
                    q = round(rnd.uniform(0.50, 1.00), 2)
                    for m in mtypes:
                        v = gen_value(study_id, m, rnd)
                        w.writerow([study_id, pid, m, v, units[m], ts, site, q])
                        rows += 1
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="combined_measurements.csv")
    ap.add_argument("--participants-per-study", type=int, default=5000)
    ap.add_argument("--days-per-participant", type=int, default=4)
    ap.add_argument("--seed", type=int, default=2025)
    ap.add_argument("--split-per-study", action="store_true",
                    help="Write separate CSVs per study instead of one combined file.")
    args = ap.parse_args()

    faker = Faker()
    Faker.seed(args.seed)
    rnd = random.Random(args.seed)

    if args.split_per_study:
        total = 0
        for sid in STUDIES:
            path = f"{sid.lower()}_measurements.csv"
            total += write_file(path, faker, rnd, {sid: STUDIES[sid]},
                                args.participants_per_study, args.days_per_participant)
        print(total)
    else:
        rows = write_file(args.out, faker, rnd, STUDIES,
                          args.participants_per_study, args.days_per_participant)
        print(rows)

if __name__ == "__main__":
    main()
