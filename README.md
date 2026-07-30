# Energy Monitor

Collects whole-house energy data into one InfluxDB: solar generation read from a Sungrow inverter
over Modbus TCP, and electricity/gas consumption taken from DSMR-reader's MQTT export. Publishes
solar values to MQTT, and optionally uploads to PVOutput (every few minutes) and Mindergas (once
daily). Built as a self-maintained replacement for the third-party `bohdans/sungather` image.

## Features

- Polls the inverter every `SCAN_INTERVAL` seconds over Modbus TCP (function code 4 / input
  registers): generation power, daily and lifetime yield, DC/MPPT voltage & current, phase voltage,
  internal temperature.
- Subscribes to DSMR-reader's JSON MQTT topics for smart-meter electricity and gas readings, and
  writes those to InfluxDB too — so one database holds generation *and* consumption.
- Writes every reading to InfluxDB 3 as line protocol (`solar` measurement, `source=sungrow` tag;
  smart-meter data under `electricity` / `gas_positions` / `electricity_day_totals` with
  `source=dsmr`).
- Publishes the same values to MQTT, one topic per field under `MQTT_TOPIC_PREFIX`.
- Optionally pushes a status update to PVOutput on an interval (gated by `ENABLE_PVOUTPUT`,
  default off).
- Optionally pushes the previous day's cumulative gas meter reading to Mindergas shortly after
  midnight (gated by `ENABLE_MINDERGAS`, default off), reading the value back out of InfluxDB —
  requires DSMR-reader's own InfluxDB export to be enabled and writing into the same database.

Both external-upload flags default to `false` so this can run safely alongside an existing
uploader (e.g. `sungrow_monitor`/DSMR-reader) without double-reporting, until you're ready to cut
over.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `WARNING` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` (case-insensitive). See [Log levels](#log-levels). |
| `INVERTER_HOST` | *required* | Inverter/dongle IP address |
| `INVERTER_PORT` | `502` | Modbus TCP port |
| `INVERTER_SLAVE_ID` | `1` | Modbus slave/unit ID |
| `SCAN_INTERVAL` | `15` | Seconds between inverter polls |
| `INFLUXDB_URL` | *required* | Base URL of InfluxDB 3, e.g. `http://influxdb:8181` |
| `INFLUXDB_TOKEN` | *required* | InfluxDB auth token |
| `INFLUXDB_DATABASE` | `energy` | InfluxDB 3 database name |
| `INFLUXDB_MEASUREMENT` | `solar` | Measurement name written to |
| `MQTT_HOST` | *required* | MQTT broker hostname/IP |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USERNAME` | *(none)* | MQTT username, if required |
| `MQTT_PASSWORD` | *(none)* | MQTT password, if required |
| `MQTT_TOPIC_PREFIX` | `energy/solar` | Topic prefix; one sub-topic per field |
| `MQTT_PUBLISH_FIELDS` | *(all)* | Comma-separated register names to publish, e.g. `total_active_power,run_state`. Leave unset to publish everything. |
| `ENABLE_DSMR` | `true` | Subscribe to DSMR-reader's MQTT topics for smart-meter data |
| `DSMR_TOPIC_ELEC` | `dsmr/json/elec` | DSMR-reader JSON telegram topic |
| `DSMR_TOPIC_GAS` | `dsmr/json/gas` | DSMR-reader JSON gas consumption topic |
| `DSMR_TOPIC_DAY` | `dsmr/day-consumption` | DSMR-reader JSON day totals topic |
| `DSMR_ELEC_MEASUREMENT` | `electricity` | InfluxDB measurement for the electricity telegram |
| `DSMR_GAS_MEASUREMENT` | `gas_positions` | InfluxDB measurement for gas readings |
| `DSMR_DAY_MEASUREMENT` | `electricity_day_totals` | InfluxDB measurement for day totals |
| `DSMR_MAX_DATA_AGE` | `300` | Cached meter values older than this count as absent |
| `ENABLE_PVOUTPUT` | `false` | When `false`, logs what would be sent instead of posting |
| `PVOUTPUT_API_KEY` | *(none)* | Required if `ENABLE_PVOUTPUT=true` |
| `PVOUTPUT_SYSTEM_ID` | *(none)* | Required if `ENABLE_PVOUTPUT=true` |
| `PVOUTPUT_V1`…`PVOUTPUT_V6` | see [PVOutput mapping](#pvoutput-mapping) | Metric name uploaded as each PVOutput parameter; unset = not uploaded |
| `PVOUTPUT_INTERVAL` | `300` | Seconds between PVOutput pushes |
| `PVOUTPUT_CUMULATIVE_FLAG` | `2` | PVOutput `c1`: `1` = v1 and v3 both lifetime, `2` = only v1 lifetime, `3` = only v3 lifetime; omit for day totals. `2` is carried over from the previous SunGather setup — see the note in `app.py` |
| `PVOUTPUT_MAX_DATA_AGE` | `600` | Skip the upload if the last successful poll is older than this, rather than re-posting a stale reading under the current timestamp |
| `ENABLE_MINDERGAS` | `false` | When `false`, logs what would be sent instead of posting |
| `MINDERGAS_API_KEY` | *(none)* | Required if `ENABLE_MINDERGAS=true` |
| `MINDERGAS_HOUR` / `MINDERGAS_MINUTE` | `0` / `5` | Local time-of-day the daily Mindergas push fires |
| `MINDERGAS_RETRY_INTERVAL` | `900` | Seconds to wait before retrying a failed daily push (retries stay within `MINDERGAS_HOUR`) |
| `MINDERGAS_GAS_MEASUREMENT` | `gas_positions` | InfluxDB measurement DSMR-reader writes gas readings to |
| `MINDERGAS_GAS_FIELD` | `delivered` | Field name within that measurement |
| `TZ` | *(container default)* | Timezone for scheduling/logging |

## Log levels

`LOG_LEVEL` sets the minimum severity written to stdout; everything below it is silent. The
default, `WARNING`, is the minimal-logging setting for normal operation:

| Level | Shows | When to use it |
|---|---|---|
| `WARNING` (default) | Config problems, connection loss/refusal, a full write queue, a stale poll skipping an upload, and every caught exception (always logged at error severity with a traceback, regardless of this setting) | Normal operation. Silences the 5-second poll heartbeat and routine push confirmations, so healthy running produces no output at all. |
| `INFO` | The above, plus the recurring poll heartbeat (`Poll ok: ac=...`), startup/connection confirmations, and successful PVOutput/Mindergas pushes | Confirming the container is alive and doing what's expected, without full per-field detail. |
| `DEBUG` | The above, plus per-field detail such as a DSMR value that failed to parse and was dropped | Diagnosing a specific data problem, e.g. a smart-meter field arriving as something unexpected. |
| `ERROR` | Only config problems and caught exceptions | Quieter than the default; loses the connection-state warnings (MQTT disconnects, a full write queue), which are usually the useful signal before something breaks outright. |
| `CRITICAL` | Practically nothing — this file has no `log.critical` calls | Not useful here; included only because it's one of Python's standard levels. |

An unrecognised value (e.g. a typo) does not stop the container: it falls back to `WARNING` and
prints one line to stderr naming the invalid value and the valid options.

Change it with a redeploy (`docker compose up -d energy-monitor` after editing the compose file's
`LOG_LEVEL`) — there's no way to change it on a running container without restarting it.

## Modbus register map

Targets the **Sungrow SG5.0RS** (`device_type_code` `0x2606`, nominal 5.0 kW) — a single-phase
residential string inverter, so the hybrid-only (SH-series) register blocks don't apply. Register
addresses, datatypes and scaling are Sungrow's own published Modbus protocol definition as encoded
in [bohdan-s/SungrowClient](https://github.com/bohdan-s/SungrowClient) — referenced for
interoperability, not vendored code. See `REGISTERS` in `app.py` for the exact addresses.

Addresses there are 1-based protocol register numbers; Modbus reads are 0-based, hence the `-1`
in `poll_inverter`. All registers are read in a single input-register (function code 4) request.

This inverter family exposes no `run_state`/`system_state` register, so `run_state` is derived
from whether `total_active_power > 0`.

Values were validated against the real inverter: MPPT1 + MPPT2 DC power sums to `total_dc_power`,
and `total_active_power` (AC) sits just below it with a realistic conversion loss.

## Smart-meter data

Electricity and gas come from **DSMR-reader's own MQTT export**, not from its database, REST API or
InfluxDB export. Enable the JSON exports in DSMR-reader's admin UI (Configuration → MQTT) pointing
at the same broker this container uses, and set the topics here to match:

| Source key | Variable | DSMR-reader export | Written to measurement |
|---|---|---|---|
| `elec` | `DSMR_TOPIC_ELEC` | JSON telegram | `DSMR_ELEC_MEASUREMENT` (`electricity`) |
| `gas` | `DSMR_TOPIC_GAS` | JSON gas consumption | `DSMR_GAS_MEASUREMENT` (`gas_positions`) |
| `day` | `DSMR_TOPIC_DAY` | JSON day totals | `DSMR_DAY_MEASUREMENT` (`electricity_day_totals`) |

Each message is parsed, coerced to numbers (DSMR-reader publishes every value as a JSON string),
cached in memory for the PVOutput metrics, and written to InfluxDB. Non-numeric fields are skipped
rather than written as strings. Subscriptions are re-established on every reconnect, so a broker
restart doesn't silently leave the container deaf.

Why MQTT and not the alternatives:

- **DSMR-reader's InfluxDB export cannot work against InfluxDB 3.** Its client calls
  `find_bucket_by_name()` before writing, and InfluxDB 3 has no `/api/v2/buckets` endpoint — it
  returns 404, so the export fails before the (otherwise working) write is attempted.
- **Reading the P1 meter directly is not possible** when a ser2net-style bridge already feeds
  DSMR-reader: those endpoints accept a single client and answer `Port already in use`.
- The database and REST API would both work, but MQTT needs no schema coupling, no extra
  credentials, and no network changes — the broker connection already exists for publishing.

## PVOutput mapping

Each PVOutput parameter is assigned a **metric** by name via `PVOUTPUT_V1`…`PVOUTPUT_V6`. Metrics
are already expressed in the unit PVOutput expects, so there is no scaling or formula syntax to
get wrong. Leave a variable unset and that parameter simply isn't uploaded.

The defaults reproduce exactly what the previous SunGather setup sent:

```yaml
- PVOUTPUT_V1=daily_generation_wh    # Energy Generation  (Wh)
- PVOUTPUT_V2=generation_w           # Power Generation   (W)
- PVOUTPUT_V6=inverter_voltage_v     # Voltage            (V)
# V3 (Energy Consumption), V4 (Power Consumption) and V5 (Temperature) unset
```

### Available metrics

| Metric | Unit | Source | Notes |
|---|---|---|---|
| `daily_generation_wh` | Wh | inverter | Day total, resets at midnight |
| `total_generation_wh` | Wh | inverter | Lifetime counter |
| `generation_w` | W | inverter | Current AC output |
| `dc_power_w` | W | inverter | Combined MPPT DC input |
| `inverter_voltage_v` | V | inverter | Grid voltage as the inverter measures it |
| `inverter_temp_c` | °C | inverter | **Heatsink**, not ambient (~50 °C running) — mapping this to `v5` will skew PVOutput's insolation figures, which is why the old setup left it off |
| `grid_import_w` | W | meter (`elec`) | Currently drawn from the grid |
| `grid_export_w` | W | meter (`elec`) | Currently fed back |
| `grid_voltage_v` | V | meter (`elec`) | Voltage as the smart meter measures it |
| `total_import_wh` | Wh | meter (`elec`) | Lifetime, both tariffs summed |
| `total_export_wh` | Wh | meter (`elec`) | Lifetime, both tariffs summed |
| `daily_import_wh` | Wh | meter (`day`) | Today's import so far |
| `daily_export_wh` | Wh | meter (`day`) | Today's export so far |
| `house_power_w` | W | derived (`elec`) | `generation + import − export` — what the house actually uses |
| `total_house_energy_wh` | Wh | derived (`elec`) | Lifetime equivalent of the above |
| `daily_house_energy_wh` | Wh | derived (`day`) | Day-total equivalent of the above |

Metrics sourced from the **meter** read the cached MQTT payloads described in
[Smart-meter data](#smart-meter-data) — `elec` means `DSMR_TOPIC_ELEC`, `day` means
`DSMR_TOPIC_DAY`. No InfluxDB query is involved, so these stay usable even while InfluxDB writes
are failing.

If a required payload hasn't arrived, or is older than `DSMR_MAX_DATA_AGE`, the parameters needing
it are dropped and the upload still goes out with the inverter-only values. A stopped DSMR-reader
therefore degrades the upload rather than losing it or reporting figures from hours ago.

### Reporting consumption as well as generation

To have PVOutput show household consumption, not just generation:

```yaml
- PVOUTPUT_V1=total_generation_wh
- PVOUTPUT_V3=total_house_energy_wh
- PVOUTPUT_V4=house_power_w
- PVOUTPUT_CUMULATIVE_FLAG=1
```

Lifetime metrics are used for `v1`/`v3` here on purpose: lifetime counters never reset, so there's
no day-boundary or timezone ambiguity about which day a figure belongs to.

`PVOUTPUT_CUMULATIVE_FLAG` (PVOutput's `c1`) **must agree** with whether `v1`/`v3` are lifetime
counters — `1` = both lifetime, `2` = only `v1`, `3` = only `v3`, omitted = both are day totals. A
mismatch makes PVOutput derive the wrong energy figures, so the container checks the combination at
startup and logs a warning describing the fix. An unknown metric name is a hard error: it logs the
full list of valid names and exits rather than uploading something unintended.

## Mindergas integration

Mirrors what DSMR-reader's own exporter does
([src/dsmr_mindergas/services.py](https://github.com/dsmrreader/dsmr-reader/blob/v6/src/dsmr_mindergas/services.py)):
picks the last gas reading timestamped in the 3 hours before local midnight (i.e. the previous
day's final reading) and POSTs `{"date": <that day>, "reading": "<m3>"}` to
`https://www.mindergas.nl/api/meter_readings` with an `AUTH-TOKEN` header.

Unlike the PVOutput metrics this reads from **InfluxDB**, not the MQTT cache — it needs the value as
it stood before midnight, which requires timestamped history rather than the latest value. It reads
the gas data this container itself wrote (`MINDERGAS_GAS_MEASUREMENT` defaults to
`DSMR_GAS_MEASUREMENT`), so the two stay consistent automatically.

Timestamps are timezone-aware on purpose: InfluxDB stores UTC and interprets a naive string as UTC,
so passing naive local time would shift the window by the local offset and select the wrong day's
reading.

## Tests

`smoke_test.py` starts an in-process Modbus server serving known register values, polls it through
`app.poll_inverter`, and asserts the decoded results — including that DC input covers AC output and
that the MPPT strings account for the DC total, which catches word-order and scaling regressions.

```bash
docker build -t energy-monitor:dev .
docker run --rm -v "$PWD/smoke_test.py:/app/smoke_test.py:ro" energy-monitor:dev python3 /app/smoke_test.py
```

Both workflows run it: `pr-validate.yml` on pull requests, and `build.yml` **before** pushing the
image, so a dependency bump that breaks decoding at runtime can never reach `:latest`. That matters
because building alone proves nothing here — pymodbus 3.14 removed the `slave=` keyword in favour of
`device_id=`, which built cleanly and failed on every poll. The app now resolves that argument name
from the function signature, and the test would catch the next such change.

Note that Dependabot auto-merges anything it doesn't classify as `semver-major`, and it only waits
for status checks that branch protection marks **required**. Without branch protection on `main`,
`pr-validate` will not block a merge — the pre-push check in `build.yml` is what actually protects
the published image.

## Docker Compose

See `docker-compose.yml` in this repo for a usage example. A typical deployment has this
container is one service alongside the TimescaleDB instance it writes to,
alongside the InfluxDB and Grafana instances it feeds.
