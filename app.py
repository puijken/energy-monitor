import inspect
import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import psycopg
import requests
from pymodbus.client import ModbusTcpClient

LOG_LEVELS = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
              "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL}


def resolve_log_level(raw, default="WARNING"):
    """Map a LOG_LEVEL env value to a logging level, falling back to `default` on anything else.

    Returns (level_int, warning_message_or_None) rather than logging directly, since this runs
    before the logger below exists -- the module-level caller is responsible for printing the
    warning to stderr.
    """
    key = (raw or default).strip().upper()
    if key in LOG_LEVELS:
        return LOG_LEVELS[key], None
    return LOG_LEVELS[default], (
        f"Invalid LOG_LEVEL {key!r}; falling back to {default}. Valid values: {', '.join(LOG_LEVELS)}"
    )


# Resolved before any other env var, and without the env()/log helpers below, since the logger
# itself doesn't exist yet. Default WARNING for production: this hides the 5-second poll
# heartbeat and successful-push confirmations (all INFO), while every failure path in this file
# uses warning/error/exception and so always surfaces regardless of this setting.
_log_level, _log_level_warning = resolve_log_level(os.environ.get("LOG_LEVEL"))
if _log_level_warning:
    print(_log_level_warning, file=sys.stderr)

logging.basicConfig(level=_log_level, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("energy-monitor")


def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        log.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


def env_bool(name, default=False):
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default):
    raw = os.environ.get(name, default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.error("Environment variable %s must be an integer, got %r", name, raw)
        sys.exit(1)


INVERTER_HOST = env("INVERTER_HOST", required=True)
INVERTER_PORT = env_int("INVERTER_PORT", 502)
INVERTER_SLAVE_ID = env_int("INVERTER_SLAVE_ID", 1)
SCAN_INTERVAL = env_int("SCAN_INTERVAL", 15)

POSTGRES_HOST = env("POSTGRES_HOST", required=True)
POSTGRES_PORT = env_int("POSTGRES_PORT", 5432)
POSTGRES_DB = env("POSTGRES_DB", "energy")
POSTGRES_USER = env("POSTGRES_USER", required=True)
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", required=True)
POSTGRES_TABLE = env("POSTGRES_TABLE", "solar")

MQTT_HOST = env("MQTT_HOST", required=True)
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_USERNAME = env("MQTT_USERNAME")
MQTT_PASSWORD = env("MQTT_PASSWORD")
MQTT_TOPIC_PREFIX = env("MQTT_TOPIC_PREFIX", "energy/solar").rstrip("/")
# How long to allow for the broker connection before warning, then re-warning, about it.
MQTT_WARN_AFTER = env_int("MQTT_WARN_AFTER", 60)
# Comma-separated register names to publish (e.g. "total_active_power,run_state"). Empty/unset
# publishes everything.
MQTT_PUBLISH_FIELDS = {f.strip() for f in env("MQTT_PUBLISH_FIELDS", "").split(",") if f.strip()}

# Smart-meter data is taken from DSMR-reader's own MQTT export (its JSON topics), not from its
# database or REST API. That needs no DSMR-reader configuration beyond the export already being
# enabled, avoids coupling to its schema, and reaches us over the broker we're already connected
# to. Reading the P1 stream directly is not an option: the ser2net endpoint feeding DSMR-reader
# accepts a single client and answers "Port already in use" to anyone else.
ENABLE_DSMR = env_bool("ENABLE_DSMR", True)
DSMR_TOPIC_ELEC = env("DSMR_TOPIC_ELEC", "dsmr/json/elec")
DSMR_TOPIC_GAS = env("DSMR_TOPIC_GAS", "dsmr/json/gas")
DSMR_TOPIC_DAY = env("DSMR_TOPIC_DAY", "dsmr/day-consumption")
DSMR_ELEC_TABLE = env("DSMR_ELEC_TABLE", "electricity")
DSMR_GAS_TABLE = env("DSMR_GAS_TABLE", "gas_positions")
DSMR_DAY_TABLE = env("DSMR_DAY_TABLE", "electricity_day_totals")
# Cached smart-meter values older than this are treated as absent, so a stalled DSMR-reader or
# broker can't feed stale consumption figures into a PVOutput upload.
DSMR_MAX_DATA_AGE = env_int("DSMR_MAX_DATA_AGE", 300)
# If DSMR-reader's mapping includes one of these, its value is used as the point's timestamp
# instead of our receipt time. Worth enabling `read_at` on the gas topic in particular: gas is only
# measured every 5 minutes but republished on every telegram, so timestamping by measurement time
# collapses ~14k duplicate points a day down to ~275 and makes "last reading before midnight"
# exact. Falls back to receipt time when absent.
DSMR_TIME_FIELDS = ("timestamp", "read_at")
# Zigbee2MQTT smart plugs, same broker as DSMR but an unrelated subsystem: submetering a handful
# of individual circuits (e.g. "Plug A") rather than the whole house. Unlike DSMR these
# have no fixed schema across devices -- the two plug models seen on this network (Aqara
# lumi.plug.mmeu01, Tuya TS0121) each publish extra device-specific fields (auto_off /
# led_disabled_night / power_outage_count on the Aqara, indicator_mode on the Tuya) alongside a
# common subset. Only that common subset (PLUG_FIELDS below, plus state) is kept, so a plug being
# swapped for a different model needs no code change.
ENABLE_PLUGS = env_bool("ENABLE_PLUGS", False)
# "topic=label,topic=label", e.g. "zigbee2mqtt/Plug A=server". The label becomes the
# `source` column value, so keep it short and stable even if the zigbee2mqtt friendly name changes.
PLUG_TOPICS = env("PLUG_TOPICS", "")
PLUG_TABLE = env("PLUG_TABLE", "smart_plugs")
PLUG_FIELDS = ("power", "energy", "current", "voltage")
# Mirrors DSMR_MIN_INTERVAL: these plugs report on every attribute change, multiple times a
# minute, finer than a dashboard needs.
PLUG_MIN_INTERVAL = env_int("PLUG_MIN_INTERVAL", 5)


def parse_plug_topics(raw):
    """'topic=label,topic=label' -> {topic: label}.

    Malformed entries are skipped with a warning rather than raised, so one typo doesn't take
    down every other configured plug.
    """
    topics = {}
    for entry in (e.strip() for e in raw.split(",")):
        if not entry:
            continue
        topic, sep, label = entry.partition("=")
        topic, label = topic.strip(), label.strip()
        if not sep or not topic or not label:
            log.warning("Ignoring malformed PLUG_TOPICS entry %r (expected 'topic=label')", entry)
            continue
        topics[topic] = label
    return topics


PLUG_TOPIC_MAP = parse_plug_topics(PLUG_TOPICS)

# Solar, grid and house power written together as one table, from values held in memory at the
# same instant -- a live as-of join across independently-polled solar/DSMR readings would be
# more complex and costly than continuing to compute this once, at ingest. Written on *either*
# side updating (a fresh solar poll, or a fresh DSMR message), each time using the other side's
# latest cached value -- the inverter goes fully offline overnight (Modbus stops responding
# entirely, not just idles at 0), so without this house/grid would flatline right along with it
# even though the smart meter keeps reporting all night.
ENABLE_DERIVED = env_bool("ENABLE_DERIVED", True)
DERIVED_TABLE = env("DERIVED_TABLE", "power_flow")
# How long a solar poll stays "current" for power_flow purposes. Past this, the inverter is
# treated as not producing (0 W) rather than reusing an increasingly-stale reading -- comfortably
# above SCAN_INTERVAL so a single missed poll doesn't trigger it, well below "asleep all night".
SOLAR_STALE_AFTER = env_int("SOLAR_STALE_AFTER", 120)
# Minimum spacing between Postgres writes per source. DSMR-reader republishes on every telegram
# (~6s), which is finer than a dashboard needs; the in-memory cache still updates on every message,
# so PVOutput and the metrics always see the latest values regardless. 0 disables throttling.
DSMR_MIN_INTERVAL = env_int("DSMR_MIN_INTERVAL", 5)

ENABLE_PVOUTPUT = env_bool("ENABLE_PVOUTPUT", False)
PVOUTPUT_API_KEY = env("PVOUTPUT_API_KEY")
PVOUTPUT_SYSTEM_ID = env("PVOUTPUT_SYSTEM_ID")
PVOUTPUT_INTERVAL = env_int("PVOUTPUT_INTERVAL", 300)
# PVOutput's c1 flag: 1 = both v1 and v3 are lifetime energy values, 2 = only v1 is, 3 = only v3
# is. Omitting it entirely means v1/v3 are treated as day totals.
#
# 2 is carried over from the previous SunGather setup (cumulative_flag: 2) rather than derived --
# note it declares v1 a *lifetime* counter while daily_power_yields actually resets at midnight.
# That happens to produce the right daily figure (the within-day delta PVOutput computes from a
# counter starting at 0 each day equals the day's generation), but it is not what the flag means.
# Worth confirming against pvoutput.org for a day after cutover before trusting it.
PVOUTPUT_CUMULATIVE_FLAG = env_int("PVOUTPUT_CUMULATIVE_FLAG", 2)
# Don't upload a reading older than this -- otherwise a stalled Modbus connection would keep
# re-posting the last known value stamped with the current time.
PVOUTPUT_MAX_DATA_AGE = env_int("PVOUTPUT_MAX_DATA_AGE", 600)

ENABLE_MINDERGAS = env_bool("ENABLE_MINDERGAS", False)
MINDERGAS_API_KEY = env("MINDERGAS_API_KEY")
MINDERGAS_API_URL = "https://www.mindergas.nl/api/meter_readings"
MINDERGAS_HOUR = env_int("MINDERGAS_HOUR", 0)
MINDERGAS_MINUTE = env_int("MINDERGAS_MINUTE", 5)
MINDERGAS_RETRY_INTERVAL = env_int("MINDERGAS_RETRY_INTERVAL", 900)
# Where to read the gas meter position back from. Defaults line up with what this container
# writes from DSMR_TOPIC_GAS, so they only need changing if DSMR_GAS_TABLE is customised. This
# one genuinely does read from Postgres rather than the MQTT cache: Mindergas wants the last
# reading *before local midnight*, which needs timestamped history, not the current value.
MINDERGAS_GAS_TABLE = env("MINDERGAS_GAS_TABLE", DSMR_GAS_TABLE)
MINDERGAS_GAS_FIELD = env("MINDERGAS_GAS_FIELD", "delivered")

# Modbus register map for the Sungrow SG5.0RS (device_type_code 0x2606, nominal 5.0 kW) -- a
# single-phase residential string inverter with no battery or meter, so the hybrid-only
# (SH-series) register blocks don't apply.
#
# Addresses and scaling are Sungrow's own published Modbus protocol definition, as encoded in
# github.com/bohdan-s/SungrowClient (registers-sungrow.yaml) -- referenced for interoperability,
# not vendored code. Addresses are 1-based protocol register numbers; Modbus reads are 0-based,
# hence the -1 in poll_inverter. All of these live in the input-register space (function code 4).
REGISTERS = {
    "daily_power_yields": (5003, "U16", 0.1, "kWh"),
    # Whole kWh: register 5004 carries no scaling factor (unlike the 0.1-scaled variant at 5144,
    # which sits outside this scan block). Verified against the inverter -- 26261 kWh over
    # total_running_time 18169 h is 1.45 kW average while generating, right for a 5 kWp array.
    "total_power_yields": (5004, "U32", None, "kWh"),
    "total_running_time": (5006, "U32", None, "h"),
    "internal_temperature": (5008, "S16", 0.1, "C"),
    # Apparent power alongside active power gives power factor as a query-time division.
    # Read 3292 VA against 3300 W active, so this model reports it essentially unity.
    "total_apparent_power": (5009, "U32", None, "VA"),
    "mppt_1_voltage": (5011, "U16", 0.1, "V"),
    "mppt_1_current": (5012, "U16", 0.1, "A"),
    "mppt_2_voltage": (5013, "U16", 0.1, "V"),
    "mppt_2_current": (5014, "U16", 0.1, "A"),
    "total_dc_power": (5017, "U32", None, "W"),
    "phase_a_voltage": (5019, "U16", 0.1, "V"),
    "phase_a_current": (5022, "S16", 0.1, "A"),
    "total_active_power": (5031, "U32", None, "W"),
    # Sits outside the block the other registers fall in, so this widens the single Modbus read
    # by a few registers rather than costing a second round trip. Decoded into run_state below,
    # not stored under this name -- see WORK_STATES.
    "work_state_1": (5038, "U16", None, None),
}
# Deliberately absent: installed_pv_power (5016) reads 0 on this unit rather than the array
# size, so dashboards carry the 6.32 kWp figure as a constant instead. phase_b/c_voltage
# (5020/5021) and mppt_3 (5015/5016) read 0 too -- this is a single-phase, two-string model.
# Every register above already falls inside the one block poll_inverter reads, so none of
# them costs an extra Modbus round trip.

# work_state_1's value mapping, same source as REGISTERS above. Not a bitmask despite the range --
# each is a distinct sentinel value, not OR-able flags.
WORK_STATES = {
    0x0000: "Run",
    0x8000: "Stop",
    0x1300: "Key Stop",
    0x1500: "Emergency Stop",
    0x1400: "Standby",
    0x1200: "Initial Standby",
    0x1600: "Starting",
    0x9100: "Alarm Run",
    0x8100: "Derating Run",
    0x8200: "Dispatch Run",
    0x5500: "Fault",
    0x2500: "Communication Fault",
}

SCAN_START_ADDRESS = min(addr for addr, *_ in REGISTERS.values())
SCAN_END_ADDRESS = max(addr + (1 if dtype in ("U32", "S32") else 0) for addr, dtype, *_ in REGISTERS.values())
SCAN_COUNT = SCAN_END_ADDRESS - SCAN_START_ADDRESS + 1

# Fixed columns per table, matching the Postgres schema in the deploying stack's
# timescaledb/init/001_hypertables.sql. A field not listed here for its table lands in that
# table's `extra` JSONB column instead of failing the insert -- this is what keeps DSMR/plug
# ingestion schemaless from this module's perspective (enabling a new DSMR field per
# that DSMR-reader publishes needs no code change here, only a schema/dashboard change
# once it's actually wanted).
TABLE_COLUMNS = {
    POSTGRES_TABLE: set(REGISTERS) | {"run_state"},
    DSMR_ELEC_TABLE: {
        "electricity_delivered_1", "electricity_returned_1", "electricity_delivered_2",
        "electricity_returned_2", "electricity_currently_delivered",
        "electricity_currently_returned", "phase_voltage_l1",
    },
    DSMR_GAS_TABLE: {"delivered"},
    DSMR_DAY_TABLE: {"electricity_merged", "electricity_returned_merged"},
    PLUG_TABLE: set(PLUG_FIELDS) | {"state"},
    DERIVED_TABLE: {"solar_w", "grid_w", "house_w"},
}


def _f(source, values, field):
    try:
        return float(values[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise AssertionError(f"DSMR field {field!r} missing or non-numeric in {source} payload") from exc


def _elec(d, field):
    return _f("elec", d["elec"], field)


# Metrics that can be mapped onto PVOutput parameters, each already normalised to the unit
# PVOutput expects (Wh for energy, W for power) so the mapping never needs scaling syntax.
#
# Each entry is (DSMR sources required, function(inverter_values, dsmr_values)). Metrics with an
# empty tuple come straight from Modbus; the rest read the cached smart-meter payloads that arrive
# over MQTT, so no Postgres round-trip is involved and they stay usable even if writes are failing.
METRICS = {
    # --- inverter only (always available) ---
    "daily_generation_wh": ((), lambda i, d: round(i["daily_power_yields"] * 1000)),
    "total_generation_wh": ((), lambda i, d: round(i["total_power_yields"] * 1000)),
    "generation_w": ((), lambda i, d: i["total_active_power"]),
    "dc_power_w": ((), lambda i, d: i["total_dc_power"]),
    "inverter_voltage_v": ((), lambda i, d: i["phase_a_voltage"]),
    # Inverter heatsink temperature, NOT ambient -- runs ~50 C in normal operation, so mapping it
    # to v5 will skew PVOutput's insolation figures. The old SunGather config left it disabled.
    "inverter_temp_c": ((), lambda i, d: i["internal_temperature"]),
    # --- from the smart meter (grid side) ---
    "grid_import_w": (("elec",), lambda i, d: round(_elec(d, "electricity_currently_delivered") * 1000)),
    "grid_export_w": (("elec",), lambda i, d: round(_elec(d, "electricity_currently_returned") * 1000)),
    "grid_voltage_v": (("elec",), lambda i, d: _elec(d, "phase_voltage_l1")),
    "total_import_wh": (
        ("elec",),
        lambda i, d: round((_elec(d, "electricity_delivered_1") + _elec(d, "electricity_delivered_2")) * 1000),
    ),
    "total_export_wh": (
        ("elec",),
        lambda i, d: round((_elec(d, "electricity_returned_1") + _elec(d, "electricity_returned_2")) * 1000),
    ),
    "daily_import_wh": (("day",), lambda i, d: round(_f("day", d["day"], "electricity_merged") * 1000)),
    "daily_export_wh": (("day",), lambda i, d: round(_f("day", d["day"], "electricity_returned_merged") * 1000)),
    # --- derived: what the house actually used (generation + import - export) ---
    "house_power_w": (
        ("elec",),
        lambda i, d: round(
            i["total_active_power"]
            + (_elec(d, "electricity_currently_delivered") - _elec(d, "electricity_currently_returned")) * 1000
        ),
    ),
    "daily_house_energy_wh": (
        ("day",),
        lambda i, d: round(
            i["daily_power_yields"] * 1000
            + _f("day", d["day"], "electricity_merged") * 1000
            - _f("day", d["day"], "electricity_returned_merged") * 1000
        ),
    ),
    "total_house_energy_wh": (
        ("elec",),
        lambda i, d: round(
            i["total_power_yields"] * 1000
            + (_elec(d, "electricity_delivered_1") + _elec(d, "electricity_delivered_2")) * 1000
            - (_elec(d, "electricity_returned_1") + _elec(d, "electricity_returned_2")) * 1000
        ),
    ),
}

# Metrics that are lifetime counters rather than day totals. PVOutput's c1 flag has to agree with
# whichever of these lands on v1/v3, or the uploaded energy figures come out wrong.
LIFETIME_METRICS = {"total_generation_wh", "total_import_wh", "total_export_wh", "total_house_energy_wh"}

# PVOUTPUT_V1..V6 name a metric each; unset means that parameter is not uploaded. The defaults
# reproduce exactly what the previous SunGather setup sent.
PVOUTPUT_MAPPING = {
    param: name
    for param, name in (
        (param, env(f"PVOUTPUT_{param.upper()}", default))
        for param, default in (("v1", "daily_generation_wh"), ("v2", "generation_w"), ("v3", ""),
                               ("v4", ""), ("v5", ""), ("v6", "inverter_voltage_v"))
    )
    if name
}

_state_lock = threading.Lock()
latest_values = {}
latest_poll_monotonic = None

# Latest smart-meter payload per source ("elec"/"gas"/"day"), each as (values, monotonic_time).
_dsmr_lock = threading.Lock()
dsmr_cache = {}
# Last (timestamp, values) written per source, so a republished-but-unchanged reading is not
# rewritten. Postgres would collapse it anyway (ON CONFLICT on the same time+source), but there
# is no point spending the write.
_dsmr_last_written = {}
# Monotonic time of the last Postgres write per source, for DSMR_MIN_INTERVAL throttling.
_dsmr_last_write_time = {}

# Monotonic time of the last Postgres write per plug label, for PLUG_MIN_INTERVAL throttling.
_plug_last_write_time = {}
# Labels already logged once at INFO on first message, so ongoing traffic doesn't repeat it.
_plug_seen = set()

# Postgres writes for smart-meter data are handed to a worker thread rather than performed inside
# the MQTT callback. paho dispatches on_message on its network thread, so a blocking write there
# stops the socket being drained and the broker discards messages for a slow consumer -- measured as
# 6 of 20 lost with a synchronous write (against the old HTTP-based InfluxDB write path, but the
# same physical constraint applies to any blocking write here). Bounded so a prolonged Postgres
# outage cannot grow without limit; oldest points are dropped first, since fresh readings matter
# more than stale ones.
_write_queue = queue.Queue(maxsize=env_int("DSMR_WRITE_QUEUE_SIZE", 2000))


def decode_register(words, offset, datatype):
    raw = words[offset]
    if datatype == "U16":
        return 0 if raw == 0xFFFF else raw
    if datatype == "S16":
        if raw in (0xFFFF, 0x7FFF):
            return 0
        return raw - 65536 if raw >= 32767 else raw
    if datatype == "U32":
        low, high = words[offset], words[offset + 1]
        if low == 0xFFFF and high == 0xFFFF:
            return 0
        return low + high * 0x10000
    if datatype == "S32":
        low, high = words[offset], words[offset + 1]
        if low == 0xFFFF and high in (0xFFFF, 0x7FFF):
            return 0
        if high >= 32767:
            return low + high * 0x10000 - 0xFFFFFFFF - 1
        return low + high * 0x10000
    raise ValueError(f"Unsupported datatype {datatype}")


# pymodbus renamed this argument from `slave` to `device_id` (3.14 dropped `slave` outright), so
# resolve it from the signature instead of hardcoding either name. Dependabot auto-merges anything
# it doesn't classify as semver-major, which has already silently moved this pin once.
_UNIT_KWARG = (
    "device_id"
    if "device_id" in inspect.signature(ModbusTcpClient.read_input_registers).parameters
    else "slave"
)


def poll_inverter(client):
    result = client.read_input_registers(
        SCAN_START_ADDRESS - 1, count=SCAN_COUNT, **{_UNIT_KWARG: INVERTER_SLAVE_ID}
    )
    if result.isError():
        raise OSError(f"Modbus read failed: {result}")
    words = result.registers

    values = {}
    for name, (address, datatype, accuracy, _unit) in REGISTERS.items():
        value = decode_register(words, address - SCAN_START_ADDRESS, datatype)
        values[name] = round(value * accuracy, 2) if accuracy else value

    # work_state_1 (see WORK_STATES) turned out to be readable on this model too, despite an
    # earlier assumption that run/system state registers were hybrid-only in Sungrow's protocol --
    # replaces a synthetic "ON"/"OFF" derived from total_active_power > 0 with the inverter's own
    # reported state. Unmapped codes are logged verbatim rather than silently dropped, so a new one
    # shows up instead of just going missing.
    work_state_raw = values.pop("work_state_1")
    values["run_state"] = WORK_STATES.get(work_state_raw, f"Unknown (0x{work_state_raw:04X})")
    return values


_pg_conn = None


def get_pg_write_connection():
    """Returns the persistent connection used by the writer thread, (re)connecting on demand.

    A single connection reused by the one writer thread -- no pool needed, since nothing else
    ever writes concurrently (mirrors the single-persistent-HTTP-session shape of the old
    InfluxDB write path). Autocommit, since each write is already its own unit of work.
    """
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD, autocommit=True,
        )
    return _pg_conn


def write_postgres(values, ts_ns, table=None, source="sungrow"):
    """Writes one point to `table`, split into known fixed columns plus `extra` for anything
    not in TABLE_COLUMNS. ON CONFLICT DO UPDATE mirrors the old InfluxDB write path's semantics:
    writing the same (time, source) again overwrites rather than silently keeping the first
    write, which matters for the rare case of a corrected re-publish at the same measurement
    timestamp (see DSMR_TIME_FIELDS)."""
    table = table or POSTGRES_TABLE
    known = TABLE_COLUMNS.get(table, set())
    direct = {k: v for k, v in values.items() if k in known}
    extra = {k: v for k, v in values.items() if k not in known}

    ts = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
    columns = ["time", "source", "extra"] + list(direct)
    params = [ts, source, json.dumps(extra) if extra else None] + list(direct.values())
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    update_cols = ["extra"] + list(direct)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    conn = get_pg_write_connection()
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT (time, source) DO UPDATE SET {set_clause}",
            params,
        )


def get_last_gas_reading(before):
    """Last (time, value) of MINDERGAS_GAS_FIELD in MINDERGAS_GAS_TABLE strictly before `before`,
    or None. No lower bound: Mindergas wants the actual final reading of the day, not merely
    "a recent-enough one", so this must keep looking back however far it takes rather than
    giving up past some fixed window. Opens a short-lived connection rather than sharing the
    writer thread's persistent one -- this runs at most once a day, from a different thread."""
    with psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT time, "{MINDERGAS_GAS_FIELD}" FROM {MINDERGAS_GAS_TABLE} '
                f"WHERE time < %s ORDER BY time DESC LIMIT 1",
                (before,),
            )
            return cur.fetchone()


def publish_mqtt(mqtt_client, values):
    for name, value in values.items():
        if MQTT_PUBLISH_FIELDS and name not in MQTT_PUBLISH_FIELDS:
            continue
        mqtt_client.publish(f"{MQTT_TOPIC_PREFIX}/{name}", value, retain=True)


def build_pvoutput_params(values):
    """Resolves PVOUTPUT_MAPPING into upload parameters.

    Any parameter needing smart-meter data is dropped if that data is missing or stale, so a
    stopped DSMR-reader degrades this to a generation-only upload instead of losing it or
    reporting figures from hours ago.
    """
    dsmr_values = get_dsmr_values() if ENABLE_DSMR else {}
    missing = {m for name in PVOUTPUT_MAPPING.values() for m in METRICS[name][0]} - dsmr_values.keys()
    if missing:
        log.warning(
            "No fresh DSMR %s data; omitting the parameters that need it", "/".join(sorted(missing))
        )

    now = datetime.now()
    params = {"d": now.strftime("%Y%m%d"), "t": now.strftime("%H:%M"), "c1": PVOUTPUT_CUMULATIVE_FLAG}
    for param, name in sorted(PVOUTPUT_MAPPING.items()):
        required, compute = METRICS[name]
        if any(m not in dsmr_values for m in required):
            continue
        try:
            params[param] = compute(values, dsmr_values)
        except Exception:
            log.exception("Could not compute %s for %s; omitting it", name, param)
    return params


def push_pvoutput(values):
    params = build_pvoutput_params(values)
    if not {"v1", "v2"} & params.keys():
        log.error("PVOutput needs at least v1 or v2; nothing uploadable in %s", params)
        return
    if not ENABLE_PVOUTPUT:
        log.info("PVOutput push skipped (ENABLE_PVOUTPUT=false), would send: %s", params)
        return
    resp = requests.post(
        "https://pvoutput.org/service/r2/addstatus.jsp",
        headers={"X-Pvoutput-Apikey": PVOUTPUT_API_KEY, "X-Pvoutput-SystemId": PVOUTPUT_SYSTEM_ID},
        data=params,
        timeout=10,
    )
    # PVOutput explains rejections in the body ("Bad request 400: Moon powered!" etc.), which
    # raise_for_status() would discard.
    if resp.status_code != 200:
        raise AssertionError(f"PVOutput rejected the upload: {resp.status_code} {resp.text}")
    log.info("Pushed to PVOutput: %s", params)


def push_mindergas():
    # Mirrors DSMR-reader's own exporter (src/dsmr_mindergas/services.py): take the last gas
    # reading strictly before local midnight -- i.e. the previous day's actual final reading --
    # and POST {"date": <that day>, "reading": "<m3>"}. The value is read back out of the
    # gas_positions table this container itself writes from DSMR_TOPIC_GAS.
    #
    # Timestamps must be timezone-aware: Postgres stores timestamptz as UTC and reads a naive
    # value as the session's local time, so passing naive local time here would be ambiguous.
    now_local = datetime.now().astimezone()
    midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    row = get_last_gas_reading(midnight)
    if row is None:
        raise AssertionError(
            f"No gas reading found before {midnight.isoformat()} "
            f"-- is ENABLE_DSMR on and is the gas topic publishing?"
        )

    _reading_time, reading = row
    payload = {"date": (midnight - timedelta(days=1)).date().isoformat(), "reading": str(reading)}

    if not ENABLE_MINDERGAS:
        log.info("Mindergas push skipped (ENABLE_MINDERGAS=false), would send: %s", payload)
        return

    resp = requests.post(
        MINDERGAS_API_URL,
        headers={"Content-Type": "application/json", "AUTH-TOKEN": MINDERGAS_API_KEY},
        data=json.dumps(payload),
        timeout=10,
    )
    if resp.status_code != 201:
        raise AssertionError(f"Unexpected status code from Mindergas: {resp.status_code} {resp.text}")
    log.info("Pushed to Mindergas: %s", payload)


def parse_timestamp_ns(raw):
    """ISO 8601 string -> epoch nanoseconds, or None if it isn't one.

    A naive value is read as local time, matching how DSMR-reader renders timestamps when its
    Django timezone is set; Postgres stores timestamptz as UTC, so the offset has to be resolved
    here rather than left implicit.
    """
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return int(parsed.timestamp() * 1_000_000_000)


def dsmr_topic_map():
    """Subscribed topic -> (cache key, Postgres table)."""
    return {
        DSMR_TOPIC_ELEC: ("elec", DSMR_ELEC_TABLE),
        DSMR_TOPIC_GAS: ("gas", DSMR_GAS_TABLE),
        DSMR_TOPIC_DAY: ("day", DSMR_DAY_TABLE),
    }


def get_dsmr_values():
    """Cached smart-meter payloads, excluding any source that has gone stale."""
    now = time.monotonic()
    with _dsmr_lock:
        return {key: values for key, (values, seen) in dsmr_cache.items() if now - seen <= DSMR_MAX_DATA_AGE}


def _on_mqtt_message(client, userdata, message):
    entry = dsmr_topic_map().get(message.topic)
    if entry is not None:
        _handle_dsmr_message(message, entry)
        return
    plug_label = PLUG_TOPIC_MAP.get(message.topic)
    if plug_label is not None:
        _handle_plug_message(message, plug_label)


def _handle_dsmr_message(message, entry):
    key, table = entry
    try:
        # DSMR-reader publishes every value as a JSON string, so coerce to float here and drop
        # anything non-numeric rather than letting it reach Postgres as a value for a
        # double-precision column.
        payload = json.loads(message.payload.decode("utf-8"))
        values, ts_ns = {}, None
        for field, raw in payload.items():
            if field in DSMR_TIME_FIELDS and ts_ns is None:
                ts_ns = parse_timestamp_ns(raw)
                if ts_ns is not None:
                    continue  # consumed as the point's time, not stored as a field
            try:
                values[field] = float(raw)
            except (TypeError, ValueError):
                log.debug("Ignoring non-numeric DSMR field %s=%r on %s", field, raw, message.topic)
        if not values:
            return
    except Exception:
        log.exception("Could not parse DSMR payload on %s", message.topic)
        return

    with _dsmr_lock:
        first = key not in dsmr_cache
        dsmr_cache[key] = (values, time.monotonic())
    if first:
        log.info(
            "Receiving DSMR %s data on %s: %s (timestamped by %s)",
            key,
            message.topic,
            ", ".join(sorted(values)),
            "measurement time" if ts_ns is not None else "receipt time",
        )

    # A reading republished unchanged carries no new information.
    if ts_ns is not None and _dsmr_last_written.get(key) == (ts_ns, values):
        return
    # Throttle per source. Deliberately after the cache update above, so throttling only reduces
    # what is stored -- never what PVOutput and the metrics can see.
    now = time.monotonic()
    if DSMR_MIN_INTERVAL and now - _dsmr_last_write_time.get(key, 0.0) < DSMR_MIN_INTERVAL:
        return
    _dsmr_last_write_time[key] = now
    if ts_ns is not None:
        _dsmr_last_written[key] = (ts_ns, dict(values))

    write_ts_ns = ts_ns if ts_ns is not None else time.time_ns()
    queue_write(values, write_ts_ns, table, "dsmr")

    # Also refresh power_flow here, not just from the solar poll loop -- the smart meter keeps
    # reporting all night even when the inverter is asleep and Modbus stops responding entirely,
    # so this is what keeps house/grid from flatlining along with solar.
    if key == "elec" and ENABLE_DERIVED:
        try:
            queue_write(
                derived_fields({"total_active_power": _solar_power_for_derived()}, get_dsmr_values()),
                write_ts_ns,
                DERIVED_TABLE,
                "derived",
            )
        except Exception:
            log.exception("Could not compute derived power flow from DSMR update")


def _handle_plug_message(message, label):
    try:
        payload = json.loads(message.payload.decode("utf-8"))
    except Exception:
        log.exception("Could not parse plug payload on %s", message.topic)
        return

    values = {}
    for field in PLUG_FIELDS:
        if field not in payload:
            continue
        try:
            values[field] = float(payload[field])
        except (TypeError, ValueError):
            log.debug("Ignoring non-numeric plug field %s=%r on %s", field, payload[field], message.topic)
    if "state" in payload:
        values["state"] = str(payload["state"])
    if not values:
        return

    if label not in _plug_seen:
        _plug_seen.add(label)
        log.info("Receiving plug data for %s on %s: %s", label, message.topic, ", ".join(sorted(values)))

    now = time.monotonic()
    if PLUG_MIN_INTERVAL and now - _plug_last_write_time.get(label, 0.0) < PLUG_MIN_INTERVAL:
        return
    _plug_last_write_time[label] = now

    queue_write(values, time.time_ns(), PLUG_TABLE, label)


def _on_mqtt_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code != 0:
        log.warning("MQTT connection refused: %s", reason_code)
        return
    log.info("MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
    if ENABLE_DSMR:
        # (Re)subscribe on every connect, so a broker restart doesn't silently leave us deaf.
        # QoS 1: at QoS 0 the broker is free to discard messages for a consumer that is slow to
        # drain its socket, which loses readings outright.
        for topic in dsmr_topic_map():
            client.subscribe(topic, qos=1)
        log.info("Subscribed to DSMR topics: %s", ", ".join(dsmr_topic_map()))
    if ENABLE_PLUGS:
        for topic in PLUG_TOPIC_MAP:
            client.subscribe(topic, qos=1)
        log.info("Subscribed to plug topics: %s", ", ".join(PLUG_TOPIC_MAP))


def _on_mqtt_disconnect(client, userdata, flags, reason_code, properties=None):
    log.warning("MQTT disconnected (%s); paho will keep retrying in the background", reason_code)


def mqtt_connect():
    """Starts an MQTT client that connects in the background.

    connect_async means a broker that's down at startup (or restarted later) never takes the
    inverter polling or Postgres writes down with it -- paho retries on its own network thread,
    and publishes while disconnected are dropped rather than raising.
    """
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = _on_mqtt_connect
    client.on_disconnect = _on_mqtt_disconnect
    client.on_message = _on_mqtt_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


def mqtt_watchdog_loop(mqtt_client):
    """Warns while the broker is unreachable.

    paho's connect_async retries silently forever, so without this a blocked port or wrong host is
    indistinguishable from a healthy connection -- nothing is logged either way.
    """
    warned = False
    while True:
        time.sleep(MQTT_WARN_AFTER)
        if mqtt_client.is_connected():
            warned = False
            continue
        if not warned:
            log.warning(
                "Still not connected to MQTT at %s:%s after %ss. Solar values are not being "
                "published and no smart-meter data is arriving. Check the broker is reachable "
                "from this container (firewall/routing) and that the credentials are right.",
                MQTT_HOST,
                MQTT_PORT,
                MQTT_WARN_AFTER,
            )
            warned = True


def _solar_power_for_derived():
    """The inverter's current output for power_flow purposes: the real value if the last poll is
    still within SOLAR_STALE_AFTER, otherwise 0. Unlike a DSMR outage (unknown, so omitted), a
    stale solar poll reliably means "asleep, not producing" -- 0 is the correct value, not a
    guess.
    """
    with _state_lock:
        age = None if latest_poll_monotonic is None else time.monotonic() - latest_poll_monotonic
        power = latest_values.get("total_active_power")
    if power is None or age is None or age > SOLAR_STALE_AFTER:
        return 0.0
    return power


def derived_fields(values, dsmr_values):
    """Power flow at one instant: what the panels produce, what the grid does, what the house uses.

    grid_w is signed the way a meter reads: positive drawing from the grid, negative feeding back.
    Grid and house fields are omitted rather than zeroed when no fresh smart-meter data is cached,
    so a DSMR outage shows as a gap in Grafana instead of a plausible-looking 0 W.
    """
    fields = {"solar_w": float(values["total_active_power"])}
    if "elec" in dsmr_values:
        imported = METRICS["grid_import_w"][1](values, dsmr_values)
        exported = METRICS["grid_export_w"][1](values, dsmr_values)
        fields["grid_w"] = float(imported - exported)
        fields["house_w"] = float(METRICS["house_power_w"][1](values, dsmr_values))
    return fields


def queue_write(values, ts_ns, table, source):
    """Hands a point to the writer thread, dropping the oldest if the queue is saturated."""
    item = (dict(values), ts_ns, table, source)
    try:
        _write_queue.put_nowait(item)
        return
    except queue.Full:
        pass
    try:
        _write_queue.get_nowait()
    except queue.Empty:
        pass
    log.warning("Postgres write queue full; dropped the oldest queued point")
    try:
        _write_queue.put_nowait(item)
    except queue.Full:
        pass


def pg_writer_loop():
    """Drains _write_queue.

    Every Postgres write goes through here so no producer ever blocks on it. A write is a network
    round trip, which would otherwise stretch the poll interval and stall paho's network thread.
    """
    while True:
        values, ts_ns, table, source = _write_queue.get()
        try:
            write_postgres(values, ts_ns, table=table, source=source)
        except Exception:
            log.exception("Postgres write failed for %s", table)


def pvoutput_loop():
    """Fires on PVOUTPUT_INTERVAL-aligned wall-clock boundaries (:00/:05/:10 for the 300s
    default), not on a timer counted from container start -- otherwise every restart shifts the
    upload times to a new offset, which is exactly the kind of discontinuity that confused
    PVOutput's own day-total tracking during the PVOutput cutover. Same reasoning as
    mindergas_loop's wall-clock polling.
    """
    last_bucket = int(time.time() // PVOUTPUT_INTERVAL)
    while True:
        time.sleep(1)
        bucket = int(time.time() // PVOUTPUT_INTERVAL)
        if bucket == last_bucket:
            continue
        last_bucket = bucket
        with _state_lock:
            values = dict(latest_values)
            age = None if latest_poll_monotonic is None else time.monotonic() - latest_poll_monotonic
        if not values or age is None:
            continue
        if age > PVOUTPUT_MAX_DATA_AGE:
            log.warning("Skipping PVOutput push: last successful poll was %.0fs ago", age)
            continue
        try:
            push_pvoutput(values)
        except Exception:
            log.exception("PVOutput push failed")


def mindergas_loop():
    """Fires once per day once the clock reaches MINDERGAS_HOUR:MINDERGAS_MINUTE.

    Polls the wall clock rather than sleeping until the target so a container start, clock jump
    or DST change can't skip or double-fire the upload.
    """
    completed_for = None
    while True:
        now = datetime.now()
        # Deliberately scoped to the target hour, so a restart later in the day doesn't fire an
        # unexpected extra upload.
        due = now.hour == MINDERGAS_HOUR and now.minute >= MINDERGAS_MINUTE
        if due and completed_for != now.date():
            try:
                push_mindergas()
                completed_for = now.date()
            except Exception:
                log.exception("Mindergas push failed; retrying in %ss", MINDERGAS_RETRY_INTERVAL)
                time.sleep(MINDERGAS_RETRY_INTERVAL)
                continue
        time.sleep(60)


def validate_config():
    unknown = MQTT_PUBLISH_FIELDS - set(REGISTERS) - {"run_state"}
    if unknown:
        log.warning(
            "MQTT_PUBLISH_FIELDS contains unknown field(s) %s -- they will never be published. "
            "Known fields: %s",
            ", ".join(sorted(unknown)),
            ", ".join(sorted(set(REGISTERS) | {"run_state"})),
        )

    if ENABLE_PVOUTPUT and not (PVOUTPUT_API_KEY and PVOUTPUT_SYSTEM_ID):
        log.error("ENABLE_PVOUTPUT is set but PVOUTPUT_API_KEY / PVOUTPUT_SYSTEM_ID are missing")
        sys.exit(1)

    bad = {p: n for p, n in PVOUTPUT_MAPPING.items() if n not in METRICS}
    if bad:
        log.error(
            "Unknown metric(s) in PVOutput mapping: %s. Available metrics: %s",
            ", ".join(f"{p}={n}" for p, n in sorted(bad.items())),
            ", ".join(sorted(METRICS)),
        )
        sys.exit(1)

    # c1 declares which of v1/v3 are lifetime counters; a mismatch silently corrupts the energy
    # figures PVOutput derives, so surface it loudly rather than letting it upload.
    for param, lifetime_flags in (("v1", (1, 2)), ("v3", (1, 3))):
        name = PVOUTPUT_MAPPING.get(param)
        if not name:
            continue
        is_lifetime = name in LIFETIME_METRICS
        declared = PVOUTPUT_CUMULATIVE_FLAG in lifetime_flags
        if is_lifetime != declared:
            log.warning(
                "%s=%s is a %s value but PVOUTPUT_CUMULATIVE_FLAG=%s declares it %s. "
                "Set the flag to %s, or map %s to a %s metric.",
                param,
                name,
                "lifetime" if is_lifetime else "day-total",
                PVOUTPUT_CUMULATIVE_FLAG,
                "lifetime" if declared else "a day total",
                "1 (both lifetime), 2 (only v1), 3 (only v3)",
                param,
                "day-total" if declared else "lifetime",
            )

    needs_dsmr = sorted({m for n in PVOUTPUT_MAPPING.values() for m in METRICS[n][0]})
    if needs_dsmr and not ENABLE_DSMR:
        log.error(
            "PVOutput mapping needs smart-meter data (%s) but ENABLE_DSMR is false",
            ", ".join(needs_dsmr),
        )
        sys.exit(1)
    log.info(
        "PVOutput mapping: %s%s",
        ", ".join(f"{p}={n}" for p, n in sorted(PVOUTPUT_MAPPING.items())) or "(none)",
        f" | needs smart-meter data from MQTT: {', '.join(needs_dsmr)}" if needs_dsmr else "",
    )
    if ENABLE_MINDERGAS and not MINDERGAS_API_KEY:
        log.error("ENABLE_MINDERGAS is set but MINDERGAS_API_KEY is missing")
        sys.exit(1)

    if ENABLE_PLUGS and not PLUG_TOPIC_MAP:
        log.error("ENABLE_PLUGS is set but PLUG_TOPICS has no valid entries")
        sys.exit(1)


def main():
    log.info(
        "Starting energy-monitor: inverter=%s:%s scan_interval=%ss dsmr=%s plugs=%s(%d) pvoutput=%s mindergas=%s",
        INVERTER_HOST,
        INVERTER_PORT,
        SCAN_INTERVAL,
        ENABLE_DSMR,
        ENABLE_PLUGS,
        len(PLUG_TOPIC_MAP),
        ENABLE_PVOUTPUT,
        ENABLE_MINDERGAS,
    )
    validate_config()

    global latest_poll_monotonic

    modbus_client = ModbusTcpClient(INVERTER_HOST, port=INVERTER_PORT, timeout=10)
    mqtt_client = mqtt_connect()

    threading.Thread(target=mqtt_watchdog_loop, args=(mqtt_client,), daemon=True).start()
    threading.Thread(target=pg_writer_loop, daemon=True).start()
    threading.Thread(target=pvoutput_loop, daemon=True).start()
    threading.Thread(target=mindergas_loop, daemon=True).start()

    while True:
        cycle_started = time.monotonic()
        try:
            if not modbus_client.connected and not modbus_client.connect():
                raise OSError(f"Could not connect to inverter at {INVERTER_HOST}:{INVERTER_PORT}")

            values = poll_inverter(modbus_client)
            ts_ns = time.time_ns()
            with _state_lock:
                latest_values.clear()
                latest_values.update(values)
                latest_poll_monotonic = time.monotonic()

            log.info(
                "Poll ok: ac=%sW dc=%sW daily=%skWh temp=%sC",
                values["total_active_power"],
                values["total_dc_power"],
                values["daily_power_yields"],
                values["internal_temperature"],
            )

            # Each sink is independent: a Postgres outage must not stop MQTT updates (the
            # physical display reads those) and vice versa. The Postgres write is queued rather
            # than performed here, so its latency never stretches the poll interval.
            queue_write(values, ts_ns, POSTGRES_TABLE, "sungrow")
            if ENABLE_DERIVED:
                try:
                    queue_write(
                        derived_fields(values, get_dsmr_values() if ENABLE_DSMR else {}),
                        ts_ns,
                        DERIVED_TABLE,
                        "derived",
                    )
                except Exception:
                    log.exception("Could not compute derived power flow")
            try:
                publish_mqtt(mqtt_client, values)
            except Exception:
                log.exception("MQTT publish failed")
        except Exception:
            log.exception("Inverter poll failed")

        # Sleep only for the remainder of the interval, so the cadence stays SCAN_INTERVAL rather
        # than SCAN_INTERVAL plus however long the work took.
        time.sleep(max(0.0, SCAN_INTERVAL - (time.monotonic() - cycle_started)))


if __name__ == "__main__":
    main()
