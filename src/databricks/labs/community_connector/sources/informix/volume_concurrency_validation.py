"""Opt-in distributed validation for Informix shared-state Volume semantics."""

from __future__ import annotations

import os
import secrets
import socket
from collections.abc import Iterator
from typing import Any


def validate_volume_concurrency(
    spark: Any, location: str, *, workers: int = 16, minimum_hosts: int = 2
) -> list[dict[str, object]]:
    """Require one mkdir winner across multiple Spark Python worker hosts.

    Run this from a Databricks compute environment that matches the target
    pipeline. The supplied location must be a writable Unity Catalog Volume.
    """

    if workers < 2:
        raise ValueError("workers must be >= 2")
    root = os.path.join(location.rstrip("/"), f".informix-distributed-{secrets.token_hex(8)}")
    contender = os.path.join(root, "exclusive")
    renamed = os.path.join(root, "renamed")
    os.makedirs(root, mode=0o700)
    def compete(batches: Iterator[Any]) -> Iterator[Any]:
        import pandas as pd

        rows = []
        for batch in batches:
            for task_id in batch["task_id"]:
                try:
                    os.mkdir(contender, mode=0o700)
                    won = True
                except FileExistsError:
                    won = False
                rows.append(
                    {
                        "host": socket.gethostname(),
                        "pid": os.getpid(),
                        "task_id": int(task_id),
                        "won": won,
                    }
                )
        yield pd.DataFrame(rows)

    try:
        frame = spark.createDataFrame([(index,) for index in range(workers)], ["task_id"])
        result = (
            frame.repartition(workers)
            .mapInPandas(
                compete,
                "host string, pid long, task_id long, won boolean",
            )
            .collect()
        )
        records = [row.asDict(recursive=True) for row in result]
        winners = [record for record in records if record["won"]]
        hosts = {str(record["host"]) for record in records}
        if len(winners) != 1:
            raise RuntimeError(
                f"Volume exclusive mkdir had {len(winners)} winners across {len(records)} tasks"
            )
        if len(hosts) < minimum_hosts:
            raise RuntimeError(
                f"Validation used {len(hosts)} worker host(s), fewer than {minimum_hosts}"
            )
        os.rename(contender, renamed)
        if os.path.exists(contender) or not os.path.isdir(renamed):
            raise RuntimeError("Volume rename was not immediately visible to the driver")
        marker = os.path.join(renamed, "visible")
        with open(marker, "x", encoding="utf-8") as handle:
            handle.write("visible")

        def observe(batches: Iterator[Any]) -> Iterator[Any]:
            import pandas as pd

            rows = []
            for batch in batches:
                for task_id in batch["task_id"]:
                    rows.append(
                        {
                            "host": socket.gethostname(),
                            "task_id": int(task_id),
                            "visible": (
                                not os.path.exists(contender)
                                and os.path.isdir(renamed)
                                and os.path.isfile(marker)
                            ),
                        }
                    )
            yield pd.DataFrame(rows)

        observed = (
            frame.repartition(workers)
            .mapInPandas(observe, "host string, task_id long, visible boolean")
            .collect()
        )
        if not observed or any(not row["visible"] for row in observed):
            raise RuntimeError("Volume rename was not immediately visible to every worker")
        observed_hosts = {str(row["host"]) for row in observed}
        if len(observed_hosts) < minimum_hosts:
            raise RuntimeError(
                "Rename validation did not execute on the required number of worker hosts"
            )
        return records
    finally:
        try:
            os.unlink(os.path.join(renamed, "visible"))
        except OSError:
            pass
        for path in (renamed, contender, root):
            try:
                os.rmdir(path)
            except OSError:
                pass


__all__ = ["validate_volume_concurrency"]
