import asyncio
import json
from typing import Optional, List, Dict, Tuple

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from pydantic import ValidationError

from . import cassandra
from .asyncio import execute_async
from ...config import config
from ...logcfg.logcfg import log, logexc
from ...metrics.metrics import CASSANDRA_TIME, CASSANDRA_ERRORS, BAD_DATA_CASSANDRA
from ...models.connection import Connection
from ...models.device import Device
from ...models.errors import SLPBaseException, InvalidDataException
from ...models.service_location import ServiceLocation
from ...models.technical_eligibilities import TechnicalEligibilitiesEmbed, Eligibility

# Names for referring to prepared statements
stmt_svcloc = "svcloc"
stmt_eligibilities = "eligibilities"
stmt_both = "both"

table_name = "service_locations_v1"

field_id = "service_location_id"
field_service_location = "service_location"
field_deleted = "deleted"
field_eligibilities = "technical_eligibility"
field_ean = "connection_point_ean"
field_connection = "connection"


class ServiceLocationDBException(SLPBaseException):
    _parent: Exception

    def __init__(self, parent: Exception, *args: object) -> None:
        self._parent = parent
        super().__init__(*args)

    def __str__(self):
        return "Error accessing Service Location DB: " + str(self._parent)


class ServiceLocationDB:
    _prepared_session_id = None
    _statements = {}

    def __init__(self):
        cassandra.get_session()
        log.info("Service Location DB (Cassandra) client initialised")

    def close(self):
        cassandra.close()

    def _prepare(self) -> Session:
        """
        Prepare the statements used in this module. Automatically re-prepare if the session has changed.
        NOTE: This is executed synchronously
        """
        sess = cassandra.get_session()
        if self._prepared_session_id is None or self._prepared_session_id != sess.session_id:
            consistency = ConsistencyLevel.name_to_value[config.cassandra_consistency]
            svcloc = sess.prepare(
                f"SELECT {field_id}, {field_service_location}, {field_deleted} FROM {table_name} WHERE {field_id}=? LIMIT 1")
            svcloc.consistency_level = consistency
            eligibilities = sess.prepare(
                f"SELECT {field_id}, {field_eligibilities}, {field_deleted}, WRITETIME({field_eligibilities}) AS ts FROM {table_name} WHERE {field_id}=? LIMIT 1")
            eligibilities.consistency_level = consistency
            both = sess.prepare(
                f"SELECT  {field_id}, {field_service_location}, {field_eligibilities}, {field_deleted} FROM {table_name} WHERE {field_id}=? LIMIT 1")
            both.consistency_level = consistency
            self._statements = {
                stmt_svcloc: svcloc,
                stmt_eligibilities: eligibilities,
                stmt_both: both,
            }
            self._prepared_session_id = sess.session_id
        return sess

    async def _run_stmt(self, stmt_name: str, service_location_id: str) -> List[Dict]:
        """
        Run one of the prepared statements and return the rows
        """
        attempts = 0
        while True:
            try:
                with CASSANDRA_TIME.time(), CASSANDRA_ERRORS.count_exceptions():
                    sess = self._prepare()
                    return await execute_async(sess, self._statements[stmt_name], (service_location_id,))
            except Exception as e:
                attempts += 1
                if attempts > 1:
                    raise ServiceLocationDBException(e)
                # Force reconnection to Cassandra on the first error and try again
                cassandra.get_session(True)

    async def get_servicelocation(self, service_location_id: str, include_eligibilities: bool = False) \
            -> Optional[ServiceLocation]:
        if include_eligibilities:
            rows = await self._run_stmt(stmt_both, service_location_id)
        else:
            rows = await self._run_stmt(stmt_svcloc, service_location_id)
        try:
            return rows_to_service_location(rows, include_eligibilities)
        except InvalidDataException:
            logexc("InvalidDataException")
        return None

    async def get_servicelocation_multi(self, service_location_ids: List[str], include_eligibilities: bool = False) \
            -> Dict[str, ServiceLocation]:
        tasks = []
        for id in service_location_ids:
            tasks.append(self.get_servicelocation(id, include_eligibilities))
        results = await asyncio.gather(*tasks)
        return {r.id: r for r in results if r is not None}

    async def get_eligibilities(self, service_location_id: str) -> Tuple[List[Eligibility], float]:
        rows = await self._run_stmt(stmt_eligibilities, service_location_id)
        if len(rows) < 1:
            return [], 0
        row = rows[0]
        if not row or field_eligibilities not in row:
            return [], 0
        ts = row["ts"] / 1000000
        return decode_eligibilities(row[field_eligibilities]), ts


def rows_to_service_location(rows: List[Dict], include_eligibilities: bool) -> Optional[ServiceLocation]:
    """
    Map a row from Cassandra into a ServiceLocation object
    """
    # Ignore missing/invalid/deleted rows
    if len(rows) < 1:
        return None
    first_row = rows[0]

    service_location_id = first_row.get(field_id, None)
    service_location_json = first_row.get(field_service_location, None)
    if not service_location_id \
            or not service_location_json \
            or first_row.get(field_deleted, False):
        return None

    try:
        # Split out connections and devices and parse them separately to improve resilience against bad data
        svcloc_dict = json.loads(service_location_json)
        devices_list = svcloc_dict['devices'] if 'devices' in svcloc_dict else []
        connections_list = svcloc_dict['connections'] if 'connections' in svcloc_dict else []
        svcloc_dict['devices'] = []
        svcloc_dict['connections'] = []
        svcloc = ServiceLocation.parse_obj(svcloc_dict)

        devices = []
        connections = []
        for device_dict in devices_list:
            try:
                devices.append(Device.parse_obj(device_dict))
            except (ValidationError, ValueError) as e:
                log.warn("Found invalid device data in Service Location DB for ID " + service_location_id)
        for conn_dict in connections_list:
            try:
                connections.append(Connection.parse_obj(conn_dict))
            except (ValidationError, ValueError) as e:
                log.warn("Found invalid connection data in Service Location DB for ID " + service_location_id)
        svcloc.devices = devices
        svcloc.connections = connections

    except (ValidationError, ValueError) as e:
        BAD_DATA_CASSANDRA.inc()
        raise InvalidDataException("Service Location Database", service_location_id, e) from e

    if include_eligibilities:
        svcloc.embedded = TechnicalEligibilitiesEmbed(technical_eligibilities=[])
        if field_eligibilities in first_row:
            try:
                svcloc.embedded = TechnicalEligibilitiesEmbed(
                    technical_eligibilities=decode_eligibilities(first_row[field_eligibilities]))
            except (ValidationError, ValueError) as e:
                BAD_DATA_CASSANDRA.inc()
                raise InvalidDataException("Service Location Database", service_location_id, e) from e
    else:
        svcloc.embedded = None
    return svcloc


def decode_eligibilities(json_str: str) -> List[Eligibility]:
    data = json.loads(json_str)
    if data is None or not isinstance(data, dict):
        return []
    return [Eligibility(**d) for d in data.get("technicalEligibilities", [])]
