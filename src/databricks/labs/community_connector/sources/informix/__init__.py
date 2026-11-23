"""Informix source connector."""

import copyreg
import logging

from databricks.labs.community_connector.sources.informix.informix import (
    InformixLakeflowConnect,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _INFORMIX_REGISTRATION_CONTEXT,
    _driver_registration,
    _install_registration,
    _replay_reader_range,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    TABLE_NAME,
    LakeflowPartitionedStreamReader,
    LakeflowStreamReader,
)


def _take_up_registration(reader) -> None:
    # What the merged single file does to the framework's readers as it defines them:
    # each reader's connector takes the registration's scope and what the update was
    # started for.
    scope = _INFORMIX_REGISTRATION_CONTEXT["scope"]
    if scope is None:
        return
    reader.lakeflow_connect.set_registration_scope(scope)
    reader.lakeflow_connect.set_update_request(
        _INFORMIX_REGISTRATION_CONTEXT["request"], _INFORMIX_REGISTRATION_CONTEXT.get("reset_at")
    )
    role = "delete" if reader.lakeflow_connect._is_delete_flow() else "upsert"
    # 10: Informix CDC reader initialized: scope={} table={} role={}
    table = reader.options.get(TABLE_NAME, "<unknown>")
    logging.getLogger(__name__).info(_m(10, scope, table, role))


# PySpark's DataSource API requires camelCase method names.
# pylint: disable=invalid-name
class InformixStreamReader(LakeflowStreamReader):
    """The framework's simple stream reader with the connector's hooks: a triggered run
    is told to the connector, and a replay stops at the end Spark logged for it."""

    def __init__(self, options, schema, lakeflow_connect):
        super().__init__(options, schema, lakeflow_connect)
        _take_up_registration(self)

    def prepareForTriggerAvailableNow(self) -> None:
        self.lakeflow_connect.prepare_for_trigger_available_now()

    def readBetweenOffsets(self, start: dict, end: dict):
        # The framework's replay is an unbounded read whose end is thrown away, while
        # Spark commits its own: a fresh read that stops sooner would drop what lies
        # between. A staged snapshot's first batch is served from its stage, as in the
        # merged file (_replay_reader_range).
        return _replay_reader_range(self, start, end)


class InformixPartitionedStreamReader(LakeflowPartitionedStreamReader):
    """The framework's partitioned stream reader with the connector's hooks. Its replay
    goes through get_partitions(start, end), which the connector bounds itself."""

    def __init__(self, options, schema, lakeflow_connect):
        super().__init__(options, schema, lakeflow_connect)
        _take_up_registration(self)

    def prepareForTriggerAvailableNow(self) -> None:
        self.lakeflow_connect.prepare_for_trigger_available_now()


class _InformixSourceType(type(LakeflowSource)):
    """The type of InformixDataSource, so that pickling the class can be told from
    pickling any other (see _pickled_source)."""


class InformixDataSource(LakeflowSource, metaclass=_InformixSourceType):
    """The connector as a Spark data source, registered by class from the installed
    package: ``spark.dataSource.register(InformixDataSource)``.

    Spark pickles a class from a package by name, and every process that uses it imports
    the connector afresh. Only the driver can tell which pipeline and update this is and
    hold workspace credentials, so pickling the class there gathers them and sends them
    along; unpickling it installs them (see _pickled_source).
    """

    _lakeflow_connect_cls = InformixLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "informix"

    def streamReader(self, schema):
        if self.lakeflow_connect.is_partitioned(self.options[TABLE_NAME]):
            return InformixPartitionedStreamReader(self.options, schema, self.lakeflow_connect)
        # Not partitioned: the base raises, and Spark asks for the simple reader.
        return super(LakeflowSource, self).streamReader(schema)

    def simpleStreamReader(self, schema):
        return InformixStreamReader(self.options, schema, self.lakeflow_connect)


def _active_spark_session():
    try:
        from pyspark.sql import SparkSession  # noqa: PLC0415

        # getActiveSession is the calling thread's own; active() falls back to the
        # process's session, and raises where there is none (a worker).
        return SparkSession.getActiveSession() or SparkSession.active()
    except Exception:  # pylint: disable=broad-except
        return None


def _restored_source(state):
    _install_registration(state)
    return InformixDataSource


def _pickled_source(cls):
    # Called when Spark pickles the class: on the driver at registration, where the
    # session and the credentials are, and in any process that passes the class on. A
    # subclass defined elsewhere is pickled by its name, as any class is.
    if cls is not InformixDataSource:
        return cls.__qualname__
    return _restored_source, (_driver_registration(_active_spark_session()),)


copyreg.pickle(_InformixSourceType, _pickled_source)


__all__ = [
    "InformixLakeflowConnect",
    "InformixDataSource",
    "InformixStreamReader",
    "InformixPartitionedStreamReader",
]
