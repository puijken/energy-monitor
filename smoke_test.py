"""Smoke test: start a fake inverter, poll it through app.poll_inverter, check the decoded values.

Exists because building the image proves nothing about runtime. A dependency bump that renames a
pymodbus argument, changes a datastore class or alters register decoding still builds cleanly and
only fails when it first talks to hardware -- which has already happened once (pymodbus 3.14
dropped the `slave` keyword in favour of `device_id`).

Run with no arguments; exits non-zero on the first failed expectation.
"""
import os
import sys
import threading
import time

os.environ.setdefault("INVERTER_HOST", "127.0.0.1")
os.environ.setdefault("INVERTER_PORT", "15020")
os.environ.setdefault("INFLUXDB_URL", "http://127.0.0.1:1")
os.environ.setdefault("INFLUXDB_TOKEN", "smoke")
os.environ.setdefault("MQTT_HOST", "127.0.0.1")

from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext  # noqa: E402
from pymodbus.server import StartTcpServer  # noqa: E402

import app  # noqa: E402

PORT = int(os.environ["INVERTER_PORT"])

# Raw register values, indexed by wire address (protocol address - 1), chosen so every decode path
# is exercised: U16 with scaling, S16, and U32 low/high word assembly.
RAW = {
    5002: 83,       # daily_power_yields   U16 x0.1 -> 8.3 kWh
    5003: 26261,    # total_power_yields   U32 low
    5004: 0,        #                      U32 high -> 26261 kWh (unscaled)
    5007: 571,      # internal_temperature S16 x0.1 -> 57.1 C
    5010: 2567,     # mppt_1_voltage       -> 256.7 V
    5011: 29,       # mppt_1_current       -> 2.9 A
    5012: 2317,     # mppt_2_voltage       -> 231.7 V
    5013: 95,       # mppt_2_current       -> 9.5 A
    5016: 2968,     # total_dc_power       U32 low
    5017: 0,        #                      U32 high -> 2968 W
    5018: 2378,     # phase_a_voltage      -> 237.8 V
    5030: 2888,     # total_active_power   U32 low
    5031: 0,        #                      U32 high -> 2888 W
}
EXPECTED = {
    "daily_power_yields": 8.3,
    "total_power_yields": 26261,
    "internal_temperature": 57.1,
    "mppt_1_voltage": 256.7,
    "mppt_1_current": 2.9,
    "mppt_2_voltage": 231.7,
    "mppt_2_current": 9.5,
    "total_dc_power": 2968,
    "phase_a_voltage": 237.8,
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


def main():
    threading.Thread(target=serve, daemon=True).start()

    from pymodbus.client import ModbusTcpClient

    client = ModbusTcpClient("127.0.0.1", port=PORT, timeout=5)
    time.sleep(0.5)  # let the server bind first, so the log isn't cluttered with a refused connect
    for _ in range(30):
        if client.connect():
            break
        time.sleep(0.5)
    else:
        print("FAIL: could not connect to the test inverter", file=sys.stderr)
        return 1

    values = app.poll_inverter(client)
    client.close()

    failures = []
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

    # Line protocol must stay parseable and consistently typed.
    line = app.line_protocol_field("total_active_power", ac)
    if line != "total_active_power=2888.0":
        failures.append(f"  numeric line protocol changed: {line}")
    if app.line_protocol_field("run_state", "ON") != 'run_state="ON"':
        failures.append("  string line protocol is not quoted")

    if failures:
        print(f"SMOKE TEST FAILED ({len(failures)}):", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1

    print(f"smoke test passed: {len(EXPECTED)} values decoded, unit kwarg={app._UNIT_KWARG!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
