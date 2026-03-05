from __future__ import annotations

import argparse

from app.db.session import create_schema
from app.worker.tasks import task_import_csv
from app.db.session import get_db


def run_import_csv(path: str) -> None:
    with get_db() as db:
        task_import_csv(db, "manual", path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["upgrade", "import-csv"])
    parser.add_argument("path", nargs="?")
    args = parser.parse_args()

    if args.command == "upgrade":
        create_schema()
        return

    if args.command == "import-csv" and args.path:
        run_import_csv(args.path)


if __name__ == "__main__":
    main()
