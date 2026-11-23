#!/usr/bin/env python3
"""List the Informix connector's unused Lakebase projects, and delete them on request.

A pipeline no longer does this on its reads (state.gc.drop.unused.projects): run it by hand,
as the identity that provisions the connector's projects, with that identity's workspace
credentials in the environment (the Databricks SDK's default authentication) or in the
options file as lakebase.workspace.host and lakebase.workspace.token.

The options file is a JSON object of one connection's options: its hostname, port and
server name the connection's own project, which is never touched, and its lakebase.*
options say how to reach the others. A project is unused when its connector state shows no
use within --retention-days; one another identity provisioned, one holding no connector
state, and one whose state is in another database are left alone. Without --delete the run
only lists what it would delete.

    python lakebase_project_gc_test_tool.py --options connection.json
    python lakebase_project_gc_test_tool.py --options connection.json --delete
"""

import argparse
import json
import sys

from databricks.labs.community_connector.sources.informix import lakebase_state
from databricks.labs.community_connector.sources.informix.informix import (
    PurePythonInformixBridge,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--options", required=True, help="JSON file of the connection's options")
    parser.add_argument(
        "--retention-days",
        type=int,
        default=90,
        help="days without use after which a project counts as unused (default 90)",
    )
    parser.add_argument("--delete", action="store_true", help="delete the unused projects")
    args = parser.parse_args(argv)
    if args.retention_days < 1:
        parser.error("--retention-days must be at least 1")
    with open(args.options, encoding="utf-8") as handle:
        options = {str(key).lower(): str(value) for key, value in json.load(handle).items()}
    identity = PurePythonInformixBridge._connection_identity_from(options)
    state = lakebase_state.LakebaseState(options, identity)
    print(f"own project (kept): {state.project_id}")
    found = lakebase_state.unused_projects(state, args.retention_days, delete=args.delete)
    for project in found:
        print(f"{'deleted' if args.delete else 'unused'}: {project}")
    if not found:
        print("no unused projects")
    elif not args.delete:
        print("dry run: run again with --delete to delete them")
    return 0


if __name__ == "__main__":
    sys.exit(main())
