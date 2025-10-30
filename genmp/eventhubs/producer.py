import logging
import json
import azure.functions as func
import asyncio
from genmp.function_app import app
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import threading
import datetime
import os
try:
    # Prefer shared helper for consistent environment handling
    from shared.helper_functions import get_env_variable  # type: ignore
except Exception:
    # Fallback to a minimal local implementation matching consumer.py's fallback
    def get_env_variable(key, default=None):
        return os.environ.get(key, default)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Note: background/sample run() removed — producer only supports immediate sends now.

# Background producer support
_producer_thread = None
_producer_stop_event = None

# Status metrics (thread-safe)
_status_lock = threading.Lock()
_last_send_time = None  # ISO8601 UTC string or None
_consecutive_failures = 0
_total_batches_sent = 0
_producer_connected = False
_connected_since = None

# last_alert_time holds a timezone-aware datetime or None
_last_alert_time = None

# Configuration from environment (with safe defaults), use shared helper
try:
    CONNECTIVITY_CHECK_INTERVAL_SECONDS = int(get_env_variable("producer.connectivity.check.interval") or 60)
except Exception:
    # If helper raises because var is missing, fall back to default
    try:
        CONNECTIVITY_CHECK_INTERVAL_SECONDS = int(os.environ.get("producer.connectivity.check.interval", "60"))
    except Exception:
        CONNECTIVITY_CHECK_INTERVAL_SECONDS = 60

try:
    ALERT_THROTTLE_SECONDS = int(get_env_variable("producer.alert.throttle.seconds") or 300)
except Exception:
    try:
        ALERT_THROTTLE_SECONDS = int(os.environ.get("producer.alert.throttle.seconds", "300"))
    except Exception:
        ALERT_THROTTLE_SECONDS = 300

try:
    EVENT_HUB_CONNECTION_STR = get_env_variable("producer.eventhub.connection.string") or ""
except Exception:
    try:
        EVENT_HUB_CONNECTION_STR = str(os.environ.get("producer.eventhub.connection.string", ""))
    except Exception:
        EVENT_HUB_CONNECTION_STR = ""
try:
    EVENT_HUB_NAME = get_env_variable("producer.eventhub.name") or ""
except Exception:
    try:
        EVENT_HUB_NAME = str(os.environ.get("producer.eventhub.name", ""))
    except Exception:
        EVENT_HUB_NAME = ""


def send_immediate(item) -> bool:
    """Synchronously send a single item immediately to Event Hubs.

    Uses a short-lived EventHubProducerClient and returns True if the send completed without exception.
    This is a blocking call and may increase latency in the caller thread.
    """
    global _producer_connected
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
        )
        # Use synchronous context by running an asyncio loop for the send
        async def _send():
            async with producer:
                batch = await producer.create_batch()
                body = item
                if isinstance(item, dict):
                    body = json.dumps(item, default=str)
                elif not isinstance(item, (bytes, str)):
                    body = str(item)
                batch.add(EventData(body))
                await producer.send_batch(batch)

        import asyncio as _asyncio
        _asyncio.run(_send())
        # mark connected on successful immediate send
        try:
            _set_connected(True)
        except Exception:
            pass
        return True
    except Exception:
        logger.exception("Immediate send to Event Hubs failed")
        try:
            _set_connected(False)
        except Exception:
            pass
        return False

async def is_running() -> bool:
    """Return True if the background producer thread is active."""
    await asyncio.sleep(0)
    return _producer_thread is not None and _producer_thread.is_alive()


async def get_status() -> dict:
    """Return a dict with running state and metrics.

    Keys: running (bool), last_send_time (str|null), consecutive_failures (int), total_batches_sent (int)
    """
    await asyncio.sleep(0)
    with _status_lock:
        last = _last_send_time
        failures = _consecutive_failures
        total = _total_batches_sent
    running = _producer_thread is not None and _producer_thread.is_alive()
    # read connected flag under lock for consistency
    with _status_lock:
        connected = bool(_producer_connected)
        connected_since = _connected_since
        last_alert = _last_alert_time
    return {
        "running": running,
        "connected": connected,
        "connected_since": connected_since,
        "last_alert_time": last_alert,
        "last_send_time": last,
        "consecutive_failures": failures,
        "total_batches_sent": total,
    }

# Batch sending removed. Immediate send only via `send_immediate` below.


def _producer_loop(conn_str, hub_name, stop_event):
    import asyncio as _asyncio

    _asyncio.run(_producer_run(conn_str, hub_name, stop_event))


async def _producer_run(conn_str, hub_name, stop_event):
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=conn_str, eventhub_name=hub_name
        )
        async with producer:
            # mark connected when context entered
            _set_connected(True)
            # perform an explicit connectivity check at startup
            try:
                ok = await _check_connectivity(producer)
                if not ok:
                    _set_connected(False)
            except Exception:
                logger.exception("Connectivity check failed during startup")
                _set_connected(False)

            # enter idle loop that periodically verifies connectivity
            await _producer_idle_loop(producer, stop_event)
    except Exception:
        logger.exception("Producer loop terminated unexpectedly")
        # on any startup/connection error, mark not connected
        try:
            _set_connected(False)
        except Exception:
            pass


async def _producer_idle_loop(producer: EventHubProducerClient, stop_event):
    """Idle loop that periodically checks connectivity while not stopped."""
    while not stop_event.is_set():
        try:
            # check connectivity at configured interval
            await asyncio.sleep(CONNECTIVITY_CHECK_INTERVAL_SECONDS)
            ok = await _check_connectivity(producer)
            if not ok:
                _set_connected(False)
        except Exception:
            logger.exception("Error in producer idle loop")
            _set_connected(False)
            # short backoff before retrying (don't exceed configured interval)
            await asyncio.sleep(min(5, CONNECTIVITY_CHECK_INTERVAL_SECONDS))


async def _check_connectivity(producer: EventHubProducerClient) -> bool:
    """Run a lightweight connectivity check using the provided producer client.

    Returns True if we can fetch Event Hub properties, False otherwise.
    """
    try:
        # get_eventhub_properties is a lightweight management call
        await producer.get_eventhub_properties()
        return True
    except Exception:
        return False


def _emit_alert(message: str):
    """Emit an alert when connectivity flips; placeholder for real telemetry/alerting."""
    global _last_alert_time
    try:
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        # Throttle alerts: if last alert is recent, skip emitting
        with _status_lock:
            last = _last_alert_time
            if last is not None:
                elapsed = (now_dt - last).total_seconds()
            else:
                elapsed = None
            if elapsed is not None and elapsed < ALERT_THROTTLE_SECONDS:
                logger.info(f"Alert suppressed (throttle {ALERT_THROTTLE_SECONDS}s): {message}")
                return
            _last_alert_time = now_dt
        # Log at warning level and include a short alert tag
        logger.warning(f"ALERT: {message}")
        # Integration point: emit telemetry/alerts to Application Insights or external alerting here.
    except Exception:
        logger.exception("Failed to emit alert")


def _set_connected(value: bool):
    """Set the connected flag under lock and emit alert if it flips to False."""
    global _producer_connected, _connected_since
    with _status_lock:
        prev = bool(_producer_connected)
        _producer_connected = bool(value)
        if _producer_connected and not prev:
            # Became connected
            _connected_since = datetime.datetime.now(datetime.timezone.utc).isoformat()
            logger.info("Producer marked as connected")
        elif not _producer_connected and prev:
            # Lost connectivity — emit alert
            logger.warning("Producer marked as NOT connected")
            _emit_alert("Event Hubs producer lost connectivity")

async def start():
    """Start the background producer loop in a daemon thread."""
    global _producer_thread, _producer_stop_event
    await asyncio.sleep(0)
    if _producer_thread is not None and _producer_thread.is_alive():
        logger.info("Producer already running")
        return False

    _producer_stop_event = __import__('threading').Event()
    _producer_thread = __import__('threading').Thread(target=_producer_loop, args=(EVENT_HUB_CONNECTION_STR, EVENT_HUB_NAME, _producer_stop_event), daemon=True)
    _producer_thread.start()
    logger.info("Producer thread started")
    return True

async def stop():
    """Signal the background producer to stop and wait for the thread to exit."""
    global _producer_thread, _producer_stop_event
    await asyncio.sleep(0)
    if _producer_stop_event is None:
        logger.info("Producer not running")
        return False

    _producer_stop_event.set()
    if _producer_thread is not None and _producer_thread.is_alive():
        _producer_thread.join(timeout=10)
    _producer_thread = None
    _producer_stop_event = None
    logger.info("Producer stopped")
    return True