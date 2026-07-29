import inspect
import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import requests
from pymodbus.client import ModbusTcpClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
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

INFLUXDB_URL = env("INFLUXDB_URL", required=True).rstrip("/")  # e.g. http://influxdb:8181
INFLUXDB_TOKEN = env("INFLUXDB_TOKEN", required=True)
INFLUXDB_DATABASE = env("INFLUXDB_DATABASE", "energy")
INFLUXDB_MEASUREMENT = env("INFLUXDB_MEASUREMENT", "solar")

MQTT_HOST = env("MQTT_HOST", required=True)
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_USERNAME = env("MQTT_USERNAME")
MQTT_PASSWORD = env("MQTT_PASSWORD")
MQTT_TOPIC_PREFIX = env("MQTT_TOPIC_PREFIX", "energy/solar").rstrip("/")
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
DSMR_ELEC_MEASUREMENT = env("DSMR_ELEC_MEASUREMENT", "electricity")
DSMR_GAS_MEASUREMENT = env("DSMR_GAS_MEASUREMENT", "gas_positions")
DSMR_DAY_MEASUREMENT = env("DSMR_DAY_MEASUREMENT", "electricity_day_totals")
# Cached smart-meter values older than this are treated as absent, so a stalled DSMR-reader or
# broker can't feed stale consumption figures into a PVOutput upload.
DSMR_MAX_DATA_AGE = env_int("DSMR_MAX_DATA_AGE", 300)
# If DSMR-reader's mapping includes one of these, its value is used as the InfluxDB timestamp
# instead of our receipt time. Worth enabling `read_at` on the gas topic in particular: gas is only
# measured every 5 minutes but republished on every telegram, so timestamping by measurement time
# collapses ~14k duplicate points a day down to ~275 and makes "last reading before midnight"
# exact. Falls back to receipt time when absent.
DSMR_TIME_FIELDS = ("timestamp", "read_at")
# Minimum spacing between InfluxDB writes per source. DSMR-reader republishes on every telegram
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
# writes from DSMR_TOPIC_GAS, so they only need changing if DSMR_GAS_MEASUREMENT is customised.
# This one genuinely does read from InfluxDB rather than the MQTT cache: Mindergas wants the last
# reading *before local midnight*, which needs timestamped history, not the current value.
MINDERGAS_GAS_MEASUREMENT = env("MINDERGAS_GAS_MEASUREMENT", DSMR_GAS_MEASUREMENT)
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
    "internal_temperature": (5008, "S16", 0.1, "C"),
    "mppt_1_voltage": (5011, "U16", 0.1, "V"),
    "mppt_1_current": (5012, "U16", 0.1, "A"),
    "mppt_2_voltage": (5013, "U16", 0.1, "V"),
    "mppt_2_current": (5014, "U16", 0.1, "A"),
    "total_dc_power": (5017, "U32", None, "W"),
    "phase_a_voltage": (5019, "U16", 0.1, "V"),
    "total_active_power": (5031, "U32", None, "W"),
}
SCAN_START_ADDRESS = min(addr for addr, *_ in REGISTERS.values())
SCAN_END_ADDRESS = max(addr + (1 if dtype in ("U32", "S32") else 0) for addr, dtype, *_ in REGISTERS.values())
SCAN_COUNT = SCAN_END_ADDRESS - SCAN_START_ADDRESS + 1


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
# over MQTT, so no InfluxDB round-trip is involved and they stay usable even if writes are failing.
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
# re-POSTed. InfluxDB would collapse it anyway (same measurement+tags+time), but there is no point
# spending the request.
_dsmr_last_written = {}
# Monotonic time of the last InfluxDB write per source, for DSMR_MIN_INTERVAL throttling.
_dsmr_last_write_time = {}

# InfluxDB writes for smart-meter data are handed to a worker thread rather than performed inside
# the MQTT callback. paho dispatches on_message on its network thread, so a blocking HTTP POST there
# stops the socket being drained and the broker discards messages for a slow consumer -- measured as
# 6 of 20 lost with a synchronous write. Bounded so a prolonged InfluxDB outage cannot grow without
# limit; oldest points are dropped first, since fresh readings matter more than stale ones.
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

    # This inverter family exposes no run/system state register (that block is hybrid-only in
    # Sungrow's protocol), so derive it from whether it's currently producing.
    values["run_state"] = "ON" if values["total_active_power"] > 0 else "OFF"
    return values


def line_protocol_field(name, value):
    if isinstance(value, str):
        escaped = value.replace('"', '\\"')
        return f'{name}="{escaped}"'
    # Always emit a float so a field's type can never flip between writes (an integer-looking
    # value written unsuffixed is a float to InfluxDB anyway; being explicit keeps it stable).
    return f"{name}={float(value)}"


def write_influxdb(values, ts_ns, measurement=None, source="sungrow"):
    fields = ",".join(line_protocol_field(name, value) for name, value in values.items())
    body = f"{measurement or INFLUXDB_MEASUREMENT},source={source} {fields} {ts_ns}"

    resp = requests.post(
        f"{INFLUXDB_URL}/api/v3/write_lp",
        params={"db": INFLUXDB_DATABASE, "precision": "nanosecond"},
        headers={"Authorization": f"Bearer {INFLUXDB_TOKEN}", "Content-Type": "text/plain"},
        data=body,
        timeout=10,
    )
    resp.raise_for_status()


def query_influxdb_sql(sql):
    resp = requests.post(
        f"{INFLUXDB_URL}/api/v3/query_sql",
        headers={"Authorization": f"Bearer {INFLUXDB_TOKEN}"},
        json={"db": INFLUXDB_DATABASE, "q": sql},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


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
    # reading in the 3 hours before local midnight -- i.e. the previous day's final reading --
    # and POST {"date": <that day>, "reading": "<m3>"}. The value is read back out of InfluxDB
    # (written there by DSMR-reader's InfluxDB export) rather than DSMR-reader's own database.
    #
    # Timestamps must be timezone-aware: InfluxDB stores UTC and reads a naive string as UTC,
    # so passing naive local time here would shift the window by the local UTC offset.
    now_local = datetime.now().astimezone()
    midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    window_start = midnight - timedelta(hours=3)

    sql = (
        f'SELECT time, "{MINDERGAS_GAS_FIELD}" FROM "{MINDERGAS_GAS_MEASUREMENT}" '
        f"WHERE time >= '{window_start.isoformat()}' AND time < '{midnight.isoformat()}' "
        f"ORDER BY time DESC LIMIT 1"
    )
    rows = query_influxdb_sql(sql)
    if not rows:
        raise AssertionError(
            f"No gas reading found in InfluxDB between {window_start.isoformat()} and "
            f"{midnight.isoformat()} -- is DSMR-reader's InfluxDB export enabled?"
        )

    reading = rows[0][MINDERGAS_GAS_FIELD]
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
    Django timezone is set; InfluxDB stores UTC, so the offset has to be resolved here rather than
    left implicit.
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
    """Subscribed topic -> (cache key, InfluxDB measurement)."""
    return {
        DSMR_TOPIC_ELEC: ("elec", DSMR_ELEC_MEASUREMENT),
        DSMR_TOPIC_GAS: ("gas", DSMR_GAS_MEASUREMENT),
        DSMR_TOPIC_DAY: ("day", DSMR_DAY_MEASUREMENT),
    }


def get_dsmr_values():
    """Cached smart-meter payloads, excluding any source that has gone stale."""
    now = time.monotonic()
    with _dsmr_lock:
        return {key: values for key, (values, seen) in dsmr_cache.items() if now - seen <= DSMR_MAX_DATA_AGE}


def _on_mqtt_message(client, userdata, message):
    entry = dsmr_topic_map().get(message.topic)
    if entry is None:
        return
    key, measurement = entry
    try:
        # DSMR-reader publishes every value as a JSON string, so coerce to float here and drop
        # anything non-numeric rather than letting it reach InfluxDB as a string field.
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

    item = (dict(values), ts_ns if ts_ns is not None else time.time_ns(), measurement)
    try:
        _write_queue.put_nowait(item)
    except queue.Full:
        try:
            _write_queue.get_nowait()
        except queue.Empty:
            pass
        log.warning("InfluxDB write queue full; dropped the oldest queued DSMR point")
        try:
            _write_queue.put_nowait(item)
        except queue.Full:
            pass


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


def _on_mqtt_disconnect(client, userdata, flags, reason_code, properties=None):
    log.warning("MQTT disconnected (%s); paho will keep retrying in the background", reason_code)


def mqtt_connect():
    """Starts an MQTT client that connects in the background.

    connect_async means a broker that's down at startup (or restarted later) never takes the
    inverter polling or InfluxDB writes down with it -- paho retries on its own network thread,
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


def influx_writer_loop():
    """Drains _write_queue, keeping blocking HTTP off the MQTT network thread."""
    while True:
        values, ts_ns, measurement = _write_queue.get()
        try:
            write_influxdb(values, ts_ns, measurement=measurement, source="dsmr")
        except Exception:
            log.exception("InfluxDB write failed for %s", measurement)


def pvoutput_loop():
    while True:
        time.sleep(PVOUTPUT_INTERVAL)
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


def main():
    log.info(
        "Starting energy-monitor: inverter=%s:%s scan_interval=%ss dsmr=%s pvoutput=%s mindergas=%s",
        INVERTER_HOST,
        INVERTER_PORT,
        SCAN_INTERVAL,
        ENABLE_DSMR,
        ENABLE_PVOUTPUT,
        ENABLE_MINDERGAS,
    )
    validate_config()

    global latest_poll_monotonic

    modbus_client = ModbusTcpClient(INVERTER_HOST, port=INVERTER_PORT, timeout=10)
    mqtt_client = mqtt_connect()

    threading.Thread(target=influx_writer_loop, daemon=True).start()
    threading.Thread(target=pvoutput_loop, daemon=True).start()
    threading.Thread(target=mindergas_loop, daemon=True).start()

    while True:
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

            # Each sink is independent: an InfluxDB outage must not stop MQTT updates (the
            # physical display reads those) and vice versa.
            try:
                write_influxdb(values, ts_ns)
            except Exception:
                log.exception("InfluxDB write failed")
            try:
                publish_mqtt(mqtt_client, values)
            except Exception:
                log.exception("MQTT publish failed")
        except Exception:
            log.exception("Inverter poll failed")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
