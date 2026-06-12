"""OData v4 community connector package — re-exports
``ODataLakeflowConnect`` so callers can ``from
databricks.labs.community_connector.sources.odata import
ODataLakeflowConnect`` without reaching into the implementation module."""

from databricks.labs.community_connector.sources.odata.odata import (
    ODataLakeflowConnect,
)

__all__ = ["ODataLakeflowConnect"]
