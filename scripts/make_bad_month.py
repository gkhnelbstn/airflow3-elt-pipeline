"""Build a corrupted copy of one real TLC month for the fake CDN (fault injection).

Downloads the month's yellow and green files and the zone lookup from the real TLC
CDN, and writes them under OUT in the CDN's layout. The green file and the zone
lookup are copied unchanged. In the yellow file, every Nth row (default: every
8th, 12.5%) is damaged the way a broken upstream export might damage it:

  - PULocationID is set to 999, a zone that does not exist in the zone lookup;
  - pickup and dropoff timestamps are swapped, so the trip has negative duration.

The first is caught by dbt's relationships test on fct_trips.pickup_zone_id, the
second by the valid_pct threshold on mart_file_quality. Nothing is committed: the
files are derived at run time into faults/www (git-ignored).

Run it inside the Airflow image, which has pyarrow in the warehouse venv:

    docker compose run --rm airflow-cli /opt/warehouse-venv/bin/python \
        /opt/airflow/scripts/make_bad_month.py --month 2026-07
"""

from __future__ import annotations

import argparse
import shutil
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TLC = "https://d37ci6vzurychx.cloudfront.net"


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "airflow3-elt-fault-injection"})
    with urllib.request.urlopen(request, timeout=120) as r, dest.open("wb") as fh:
        shutil.copyfileobj(r, fh)
    return dest


def corrupt(table: pa.Table, every: int) -> tuple[pa.Table, int]:
    """Damage every `every`-th row (rows 0, every, 2*every, ...). Returns the table and count."""
    hit = pa.array([i % every == 0 for i in range(table.num_rows)])
    pickup, dropoff = table["tpep_pickup_datetime"], table["tpep_dropoff_datetime"]
    zone = table["PULocationID"]
    bad_zone = pc.if_else(hit, pa.scalar(999, zone.type), zone)
    columns = {
        "PULocationID": bad_zone,
        "tpep_pickup_datetime": pc.if_else(hit, dropoff, pickup),
        "tpep_dropoff_datetime": pc.if_else(hit, pickup, dropoff),
    }
    for name, values in columns.items():
        table = table.set_column(table.schema.get_field_index(name), name, values)
    return table, pc.sum(hit.cast(pa.int64())).as_py()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--month", required=True, help="YYYY-MM, a month TLC has published")
    p.add_argument("--every", type=int, default=8, help="damage every Nth yellow row")
    p.add_argument("--out", type=Path, default=Path("/opt/airflow/faults/www"))
    args = p.parse_args()

    yellow = f"trip-data/yellow_tripdata_{args.month}.parquet"
    green = f"trip-data/green_tripdata_{args.month}.parquet"
    zones = "misc/taxi_zone_lookup.csv"
    for path in (green, zones):
        download(f"{TLC}/{path}", args.out / path)
        print(f"copied   {path}")

    original = download(f"{TLC}/{yellow}", args.out / (yellow + ".orig"))
    table, damaged = corrupt(pq.read_table(original), args.every)
    pq.write_table(table, args.out / yellow)
    original.unlink()
    print(f"damaged  {yellow}: {damaged:,} of {table.num_rows:,} rows (every {args.every}th)")


if __name__ == "__main__":
    main()
