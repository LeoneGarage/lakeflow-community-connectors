"""Informix source connector."""

from databricks.labs.community_connector.sources.informix.informix import (
    InformixLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class InformixDataSource(LakeflowSource):
    _lakeflow_connect_cls = InformixLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "informix"


__all__ = [
    "InformixLakeflowConnect",
    "InformixDataSource",
]
