"""Smoke test: exercise the app's three external dependencies without any real service.

Exists because building the image proves nothing about runtime. A dependency bump that renames a
pymodbus argument, changes a datastore class or alters register decoding still builds cleanly and
only fails when it first talks to hardware -- which has already happened once (pymodbus 3.14
dropped the `slave` keyword in favour of `device_id`).

Originally this only covered pymodbus (start an in-process Modbus server, poll it through
app.poll_inverter, check the decoded values). Dependabot auto-merges non-major bumps to
paho-mqtt and requests too, so an API change in either would also pass CI and only surface at
runtime. This file now additionally covers, all without a real broker or InfluxDB:

  - paho-mqtt: app.mqtt_connect() still returns a working client (catches a Client()/
    callback_api_version constructor change).
  - requests + the InfluxDB write path: app.write_influxdb() posts the expected path, query
    string, auth header and line-protocol body to a throwaway local http.server.
  - DSMR MQTT message handling: app._on_mqtt_message() against real captured DSMR-reader
    payloads, checking string->float coercion, timestamp-field consumption, throttling and what
    ends up on the write queue.
  - app.parse_timestamp_ns() across the timestamp shapes DSMR-reader actually emits, plus the
    two "this is not a timestamp" cases that matter (garbage, and a numeric meter reading).
  - The PVOutput metric formulas in app.METRICS, and that build_pvoutput_params() degrades to
    inverter-only output when no smart-meter data is cached.

Run with no arguments; exits non-zero on the first failed expectation. Guarded by an overall
wall-clock timeout so a hang in any check (e.g. a client that doesn't disconnect cleanly) fails
loudly instead of stalling CI.
"""
import json
import os
import signal
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty

os.environ.setdefault("INVERTER_HOST", "127.0.0.1")
os.environ.setdefault("INVERTER_PORT", "15020")
os.environ.setdefault("INFLUXDB_URL", "http://127.0.0.1:1")
os.environ.setdefault("INFLUXDB_TOKEN", "smoke")
os.environ.setdefault("MQTT_HOST", "127.0.0.1")

from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext  # noqa: E402
from pymodbus.server import StartTcpServer  # noqa: E402

import app  # noqa: E402
import paho.mqtt.client as mqtt  # noqa: E402

PORT = int(os.environ["INVERTER_PORT"])

OVERALL_TIMEOUT_S = 60

# Raw register values, indexed by wire address (protocol address - 1), chosen so every decode path
# is exercised: U16 with scaling, S16, and U32 low/high word assembly.
RAW = {
    5002: 83,       # daily_power_yields   U16 x0.1 -> 8.3 kWh
    5003: 26261,    # total_power_yields   U32 low
    5004: 0,        #                      U32 high -> 26261 kWh (unscaled)
    5005: 18173,    # total_running_time   U32 low
    5006: 0,        #                      U32 high -> 18173 h
    5007: 571,      # internal_temperature S16 x0.1 -> 57.1 C
    5008: 2990,     # total_apparent_power U32 low
    5009: 0,        #                      U32 high -> 2990 VA
    5010: 2567,     # mppt_1_voltage       -> 256.7 V
    5011: 29,       # mppt_1_current       -> 2.9 A
    5012: 2317,     # mppt_2_voltage       -> 231.7 V
    5013: 95,       # mppt_2_current       -> 9.5 A
    5016: 2968,     # total_dc_power       U32 low
    5017: 0,        #                      U32 high -> 2968 W
    5018: 2378,     # phase_a_voltage      -> 237.8 V
    5021: 122,      # phase_a_current      S16 x0.1 -> 12.2 A
    5030: 2888,     # total_active_power   U32 low
    5031: 0,        #                      U32 high -> 2888 W
}
EXPECTED = {
    "daily_power_yields": 8.3,
    "total_power_yields": 26261,
    "total_running_time": 18173,
    "internal_temperature": 57.1,
    "total_apparent_power": 2990,
    "mppt_1_voltage": 256.7,
    "mppt_1_current": 2.9,
    "mppt_2_voltage": 231.7,
    "mppt_2_current": 9.5,
    "total_dc_power": 2968,
    "phase_a_voltage": 237.8,
    "phase_a_current": 12.2,
    "total_active_power": 2888,
    "run_state": "ON",
}


def serve():
    """Serve RAW over Modbus TCP.

    Only the test harness uses pymodbus's server API, which is the volatile part: 3.14 renamed
    ModbusSlaveContext to ModbusDeviceContext, and v4 will drop these in favour of SimData/SimDevice
    (hence the deprecation warnings). If a bump breaks *this* function rather than an assertion, the
    harness needs updating, not the app.
    """
    regs = [0] * (max(RAW) + 8)
    for address, value in RAW.items():
        regs[address] = value
    # ModbusSequentialDataBlock stores at address-1, so 1 maps regs[i] to wire address i.
    block = ModbusSequentialDataBlock(1, regs)
    try:  # pymodbus >= 3.14
        from pymodbus.datastore import ModbusDeviceContext

        context = ModbusServerContext(devices=ModbusDeviceContext(ir=block), single=True)
    except ImportError:  # older pymodbus
        from pymodbus.datastore import ModbusSlaveContext

        context = ModbusServerContext(slaves=ModbusSlaveContext(ir=block, zero_mode=True), single=True)
    StartTcpServer(context=context, address=("127.0.0.1", PORT))


def check_modbus_poll(failures):
    """Original coverage: decode registers through app.poll_inverter() against a real pymodbus
    server, plus the physical-invariant and line-protocol-formatting checks that rely on it."""
    threading.Thread(target=serve, daemon=True).start()

    from pymodbus.client import ModbusTcpClient

    client = ModbusTcpClient("127.0.0.1", port=PORT, timeout=5)
    time.sleep(0.5)  # let the server bind first, so the log isn't cluttered with a refused connect
    for _ in range(30):
        if client.connect():
            break
        time.sleep(0.5)
    else:
        failures.append("  could not connect to the test inverter")
        return

    values = app.poll_inverter(client)
    client.close()

    for name, want in EXPECTED.items():
        got = values.get(name)
        if got != want:
            failures.append(f"  {name}: expected {want!r}, got {got!r}")

    # Physical consistency: DC input must cover AC output, and the MPPT strings must account for
    # the DC total. Catches sign/word-order/scaling regressions that individual values might hide.
    dc, ac = values["total_dc_power"], values["total_active_power"]
    mppt = (
        values["mppt_1_voltage"] * values["mppt_1_current"]
        + values["mppt_2_voltage"] * values["mppt_2_current"]
    )
    if not ac <= dc:
        failures.append(f"  AC output {ac}W exceeds DC input {dc}W")
    if abs(mppt - dc) / dc > 0.05:
        failures.append(f"  MPPT sum {mppt:.0f}W disagrees with total_dc_power {dc}W by >5%")

    # Apparent power can never be less than active power, and volts times amps on the AC side has
    # to land back on active power. Both catch a word-order or scaling slip on the registers added
    # later than the original block, which a single-value comparison would not.
    va = values["total_apparent_power"]
    if va < ac:
        failures.append(f"  apparent power {va}VA is below active power {ac}W")
    ac_from_vi = values["phase_a_voltage"] * values["phase_a_current"]
    if abs(ac_from_vi - ac) / ac > 0.05:
        failures.append(f"  phase A V*I {ac_from_vi:.0f}W disagrees with active power {ac}W by >5%")

    # Line protocol must stay parseable and consistently typed.
    line = app.line_protocol_field("total_active_power", ac)
    if line != "total_active_power=2888.0":
        failures.append(f"  numeric line protocol changed: {line}")
    if app.line_protocol_field("run_state", "ON") != 'run_state="ON"':
        failures.append("  string line protocol is not quoted")

    print(f"  modbus poll: {len(EXPECTED)} values decoded, unit kwarg={app._UNIT_KWARG!r}")


def check_mqtt_connect(failures):
    """paho-mqtt API coverage.

    app.mqtt_connect() uses connect_async, which schedules the connection on paho's own network
    thread and returns immediately -- it does not need (and here, does not have) a reachable
    broker. The point is only to catch a paho constructor/signature change (e.g. Client() gaining
    a required argument, or callback_api_version being renamed/removed), which would raise here
    rather than at 3am against the real broker.
    """
    try:
        client = app.mqtt_connect()
    except Exception as exc:
        failures.append(f"  app.mqtt_connect() raised {exc!r} (paho-mqtt API change?)")
        return

    try:
        if not isinstance(client, mqtt.Client):
            failures.append(
                f"  app.mqtt_connect() returned {client!r}, expected a paho.mqtt.client.Client instance"
            )
        elif not hasattr(client, "is_connected") or not hasattr(client, "publish"):
            failures.append(
                f"  app.mqtt_connect() returned {client!r}, missing expected Client API (publish/is_connected)"
            )
        else:
            print("  mqtt connect: app.mqtt_connect() returned a usable paho Client")
    finally:
        # Clean up regardless of what happened above, so a failed assertion here can't leave a
        # background reconnect thread running past the end of the process.
        try:
            client.loop_stop()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass


class _CapturingHandler(BaseHTTPRequestHandler):
    """Records a single POST's path/query/headers/body, then answers 200 OK."""

    captured = None

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        path, _, query = self.path.partition("?")
        _CapturingHandler.captured = {
            "path": path,
            "query": query,
            "headers": dict(self.headers),
            "body": body,
        }
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, fmt, *args):  # keep test output quiet
        pass


def check_influxdb_write(failures):
    """requests + write_influxdb() coverage.

    Runs a throwaway local HTTP server instead of a real InfluxDB, points app.INFLUXDB_URL at it,
    and inspects exactly what app.write_influxdb() sent -- catching both a requests API change and
    a regression in the write path itself (wrong endpoint, wrong query params, wrong auth header,
    malformed line protocol).
    """
    before = len(failures)
    _CapturingHandler.captured = None
    server = HTTPServer(("127.0.0.1", 0), _CapturingHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    orig_url, orig_token = app.INFLUXDB_URL, app.INFLUXDB_TOKEN
    app.INFLUXDB_URL = f"http://127.0.0.1:{port}"
    app.INFLUXDB_TOKEN = "test-token-abc"
    ts_ns = 1732900000123456789
    try:
        app.write_influxdb(
            {"total_active_power": 2888.0, "run_state": "ON"},
            ts_ns,
            measurement="solar_test",
            source="sungrow_test",
        )
    except Exception as exc:
        failures.append(f"  app.write_influxdb() raised {exc!r}")
    finally:
        app.INFLUXDB_URL, app.INFLUXDB_TOKEN = orig_url, orig_token
        thread.join(timeout=5)

    captured = _CapturingHandler.captured
    if captured is None:
        failures.append("  write_influxdb() never reached the test HTTP server")
        return

    if captured["path"] != "/api/v3/write_lp":
        failures.append(f"  write path: expected '/api/v3/write_lp', got {captured['path']!r}")

    qs = urllib.parse.parse_qs(captured["query"])
    if qs.get("db") != [app.INFLUXDB_DATABASE]:
        failures.append(f"  query 'db': expected {[app.INFLUXDB_DATABASE]!r}, got {qs.get('db')!r}")
    if qs.get("precision") != ["nanosecond"]:
        failures.append(f"  query 'precision': expected ['nanosecond'], got {qs.get('precision')!r}")

    auth = captured["headers"].get("Authorization")
    if auth != "Bearer test-token-abc":
        failures.append(f"  Authorization header: expected 'Bearer test-token-abc', got {auth!r}")

    tokens = captured["body"].split(" ")
    if len(tokens) != 3:
        failures.append(f"  line protocol body has unexpected shape: {captured['body']!r}")
    else:
        measurement_tags, fields_part, ts_part = tokens
        if measurement_tags != "solar_test,source=sungrow_test":
            failures.append(
                f"  line protocol measurement/tags: expected 'solar_test,source=sungrow_test', "
                f"got {measurement_tags!r}"
            )
        expected_fields = (
            f"{app.line_protocol_field('total_active_power', 2888.0)},"
            f"{app.line_protocol_field('run_state', 'ON')}"
        )
        if fields_part != expected_fields:
            failures.append(f"  line protocol fields: expected {expected_fields!r}, got {fields_part!r}")
        if ts_part != str(ts_ns):
            failures.append(f"  line protocol timestamp: expected {ts_ns}, got {ts_part!r}")

    if len(failures) == before:
        print("  influxdb write: path/query/auth/body all as expected")


class _StubMessage:
    """Mimics the paho MQTTMessage attributes app._on_mqtt_message() reads."""

    def __init__(self, topic, payload_dict):
        self.topic = topic
        self.payload = json.dumps(payload_dict).encode("utf-8")


def check_dsmr_mqtt_message(failures):
    """DSMR MQTT message parsing, using real captured DSMR-reader payloads.

    Exercises app._on_mqtt_message() end to end: JSON-string -> float coercion, the timestamp
    field (read_at) being consumed rather than stored as a value, per-source throttling, and what
    lands on the InfluxDB write queue.
    """
    before = len(failures)
    orig_min_interval = app.DSMR_MIN_INTERVAL
    app.DSMR_MIN_INTERVAL = 0  # disable throttling so both messages definitely get queued
    with app._dsmr_lock:
        orig_cache = dict(app.dsmr_cache)
        app.dsmr_cache.clear()
    orig_last_written = dict(app._dsmr_last_written)
    orig_last_write_time = dict(app._dsmr_last_write_time)
    app._dsmr_last_written.clear()
    app._dsmr_last_write_time.clear()
    drained = []
    while True:
        try:
            drained.append(app._write_queue.get_nowait())
        except Empty:
            break

    elec_payload = {
        "electricity_delivered_1": "13658.117",
        "electricity_returned_1": "5750.043",
        "electricity_delivered_2": "11190.901",
        "electricity_returned_2": "12043.452",
        "electricity_currently_delivered": "0.000",
        "electricity_currently_returned": "1.644",
        "phase_voltage_l1": "235.0",
    }
    gas_payload = {
        "read_at": "2026-07-29T11:45:04+02:00",
        "delivered": "6573.284",
        "currently_delivered": "0.072",
    }

    try:
        app._on_mqtt_message(None, None, _StubMessage(app.DSMR_TOPIC_ELEC, elec_payload))
        app._on_mqtt_message(None, None, _StubMessage(app.DSMR_TOPIC_GAS, gas_payload))

        values = app.get_dsmr_values()

        expected_elec = {k: float(v) for k, v in elec_payload.items()}
        if values.get("elec") != expected_elec:
            failures.append(f"  dsmr 'elec' cache: expected {expected_elec}, got {values.get('elec')}")

        expected_gas = {"delivered": 6573.284, "currently_delivered": 0.072}
        if "read_at" in values.get("gas", {}):
            failures.append(f"  dsmr 'gas' cache still has 'read_at' as a field: {values.get('gas')}")
        elif values.get("gas") != expected_gas:
            failures.append(f"  dsmr 'gas' cache: expected {expected_gas}, got {values.get('gas')}")

        queued = []
        while True:
            try:
                queued.append(app._write_queue.get_nowait())
            except Empty:
                break

        elec_item = next((it for it in queued if it[2] == app.DSMR_ELEC_MEASUREMENT), None)
        gas_item = next((it for it in queued if it[2] == app.DSMR_GAS_MEASUREMENT), None)

        if elec_item is None:
            failures.append(
                f"  no point enqueued for elec measurement {app.DSMR_ELEC_MEASUREMENT!r}; queued={queued}"
            )
        else:
            elec_values, _elec_ts_ns, _elec_measurement, elec_source = elec_item
            if elec_values != expected_elec:
                failures.append(f"  queued elec point values: expected {expected_elec}, got {elec_values}")
            if elec_source != "dsmr":
                failures.append(f"  queued elec point source: expected 'dsmr', got {elec_source!r}")

        if gas_item is None:
            failures.append(
                f"  no point enqueued for gas measurement {app.DSMR_GAS_MEASUREMENT!r}; queued={queued}"
            )
        else:
            gas_values, gas_ts_ns, _gas_measurement, gas_source = gas_item
            expected_gas_ts_ns = int(datetime(2026, 7, 29, 9, 45, 4, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
            if "read_at" in gas_values:
                failures.append(f"  queued gas point still has 'read_at' as a field: {gas_values}")
            elif gas_values != expected_gas:
                failures.append(f"  queued gas point values: expected {expected_gas}, got {gas_values}")
            if gas_ts_ns != expected_gas_ts_ns:
                failures.append(
                    f"  queued gas point timestamp: expected {expected_gas_ts_ns} "
                    f"(2026-07-29T09:45:04 UTC), got {gas_ts_ns}"
                )
            if gas_source != "dsmr":
                failures.append(f"  queued gas point source: expected 'dsmr', got {gas_source!r}")

        if len(failures) == before:
            print("  dsmr mqtt parsing: elec + gas payloads decoded, timestamped and queued as expected")
    finally:
        app.DSMR_MIN_INTERVAL = orig_min_interval
        with app._dsmr_lock:
            app.dsmr_cache.clear()
            app.dsmr_cache.update(orig_cache)
        app._dsmr_last_written.clear()
        app._dsmr_last_written.update(orig_last_written)
        app._dsmr_last_write_time.clear()
        app._dsmr_last_write_time.update(orig_last_write_time)
        for item in drained:  # put back anything that was queued before this check ran
            app._write_queue.put_nowait(item)


def check_parse_timestamp_ns(failures):
    """app.parse_timestamp_ns() across the shapes DSMR-reader emits, and the two non-timestamp
    cases that matter most: garbage, and a numeric meter reading (so a meter value can never be
    mistaken for a timestamp field)."""
    expected_ns = int(datetime(2026, 7, 29, 9, 45, 4, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    same_instant = {
        "2026-07-29T11:45:04+02:00": "tz-aware offset",
        "2026-07-29T09:45:04+00:00": "UTC offset",
        "2026-07-29T09:45:04Z": "Z suffix",
    }
    for raw, label in same_instant.items():
        got = app.parse_timestamp_ns(raw)
        if got != expected_ns:
            failures.append(f"  parse_timestamp_ns({raw!r}) [{label}]: expected {expected_ns}, got {got}")

    # Naive local value: parse_timestamp_ns() reads this as local time (matching how DSMR-reader
    # renders timestamps), so compute the expected value the same way rather than hardcoding a
    # timezone-dependent number.
    naive_raw = "2026-07-29T09:45:04"
    expected_naive_ns = int(datetime.fromisoformat(naive_raw).astimezone().timestamp() * 1_000_000_000)
    got_naive = app.parse_timestamp_ns(naive_raw)
    if got_naive != expected_naive_ns:
        failures.append(
            f"  parse_timestamp_ns({naive_raw!r}) [naive local]: expected {expected_naive_ns}, got {got_naive}"
        )

    for bad in ("not-a-date", "6573.284"):
        got_bad = app.parse_timestamp_ns(bad)
        if got_bad is not None:
            failures.append(f"  parse_timestamp_ns({bad!r}): expected None, got {got_bad}")

    print("  parse_timestamp_ns: offset/UTC/Z/naive all resolved; garbage and numeric strings rejected")


def check_derived_fields(failures):
    """The power_flow point written on each poll, which exists so Grafana never has to join
    measurements. Reuses the same inverter/DSMR fixtures as the metric checks below."""
    before = len(failures)
    inverter = {"total_active_power": 2888}
    dsmr = {
        "elec": {
            "electricity_currently_delivered": 0.0,
            "electricity_currently_returned": 1.644,
        }
    }

    # solar_w = 2888
    # grid_w  = (0.0 * 1000) - (1.644 * 1000) = -1644   (negative == feeding the grid)
    # house_w = 2888 + (0.0 - 1.644) * 1000  =  1244
    got = app.derived_fields(inverter, dsmr)
    for name, want in (("solar_w", 2888.0), ("grid_w", -1644.0), ("house_w", 1244.0)):
        if got.get(name) != want:
            failures.append(f"  derived {name}: expected {want}, got {got.get(name)!r}")
    if got and not all(isinstance(v, float) for v in got.values()):
        failures.append(f"  derived fields must all be floats, got {got}")

    # Without cached smart-meter data the grid/house fields must be absent, not zero -- a zero
    # would render in Grafana as a real "house using nothing" reading.
    solar_only = app.derived_fields(inverter, {})
    if set(solar_only) != {"solar_w"}:
        failures.append(f"  derived fields without DSMR: expected only solar_w, got {sorted(solar_only)}")

    if len(failures) == before:
        print("  derived fields: solar/grid/house computed together; omitted (not zeroed) without DSMR")


def check_pvoutput_metrics(failures):
    """PVOutput metric formulas in app.METRICS, and build_pvoutput_params() degrading to
    inverter-only output when no smart-meter data is cached."""
    before = len(failures)
    inverter = {
        "daily_power_yields": 8.3,
        "total_power_yields": 26261,
        "total_active_power": 2888,
        "total_dc_power": 2968,
        "phase_a_voltage": 237.8,
        "internal_temperature": 57.1,
    }
    dsmr = {
        "elec": {
            "electricity_currently_delivered": 0.0,
            "electricity_currently_returned": 1.644,
            "electricity_delivered_1": 13658.117,
            "electricity_delivered_2": 11190.901,
            "electricity_returned_1": 5750.043,
            "electricity_returned_2": 12043.452,
            "phase_voltage_l1": 235.0,
        }
    }

    # house_power_w = total_active_power + (currently_delivered - currently_returned) * 1000
    #               = 2888 + (0.0 - 1.644) * 1000 = 2888 - 1644 = 1244
    expected_house_power_w = 1244
    got_house_power_w = app.METRICS["house_power_w"][1](inverter, dsmr)
    if got_house_power_w != expected_house_power_w:
        failures.append(f"  house_power_w: expected {expected_house_power_w}, got {got_house_power_w}")

    # total_house_energy_wh = total_power_yields*1000 + (delivered_1+delivered_2)*1000
    #                                                  - (returned_1+returned_2)*1000
    #   delivered_1+delivered_2 = 13658.117 + 11190.901 = 24849.018 -> *1000 = 24849018
    #   returned_1+returned_2  =  5750.043 + 12043.452  = 17793.495 -> *1000 = 17793495
    #   total = 26261*1000 + 24849018 - 17793495 = 26261000 + 24849018 - 17793495 = 33316523
    expected_total_house_energy_wh = 33316523
    got_total_house_energy_wh = app.METRICS["total_house_energy_wh"][1](inverter, dsmr)
    if got_total_house_energy_wh != expected_total_house_energy_wh:
        failures.append(
            f"  total_house_energy_wh: expected {expected_total_house_energy_wh}, got {got_total_house_energy_wh}"
        )

    # build_pvoutput_params() must drop parameters that need smart-meter data when nothing is
    # cached, rather than erroring out or reporting stale figures.
    orig_mapping = dict(app.PVOUTPUT_MAPPING)
    with app._dsmr_lock:
        orig_cache = dict(app.dsmr_cache)
        app.dsmr_cache.clear()
    app.PVOUTPUT_MAPPING = {"v1": "daily_generation_wh", "v3": "house_power_w"}
    try:
        params = app.build_pvoutput_params(inverter)
        if "v3" in params:
            failures.append(
                f"  build_pvoutput_params() included v3 (house_power_w needs DSMR 'elec') with an "
                f"empty dsmr_cache: {params}"
            )
        if "v1" not in params:
            failures.append(f"  build_pvoutput_params() dropped v1 (inverter-only) unexpectedly: {params}")
    finally:
        app.PVOUTPUT_MAPPING = orig_mapping
        with app._dsmr_lock:
            app.dsmr_cache.clear()
            app.dsmr_cache.update(orig_cache)

    if len(failures) == before:
        print("  pvoutput metrics: house_power_w/total_house_energy_wh correct; degrades to inverter-only without DSMR")


def _timeout_handler(signum, frame):
    print(f"SMOKE TEST FAILED: exceeded overall {OVERALL_TIMEOUT_S}s timeout (a check likely hung)", file=sys.stderr)
    os._exit(1)


def main():
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(OVERALL_TIMEOUT_S)

    failures = []
    check_modbus_poll(failures)
    check_mqtt_connect(failures)
    check_influxdb_write(failures)
    check_dsmr_mqtt_message(failures)
    check_parse_timestamp_ns(failures)
    check_pvoutput_metrics(failures)
    check_derived_fields(failures)

    signal.alarm(0)

    if failures:
        print(f"SMOKE TEST FAILED ({len(failures)}):", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1

    print("smoke test passed: modbus, mqtt connect, influxdb write, dsmr parsing, timestamps, "
          "pvoutput metrics, derived fields")
    return 0


if __name__ == "__main__":
    sys.exit(main())
