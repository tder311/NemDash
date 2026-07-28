#!/usr/bin/env python3
"""
Standalone continuous-ingestion worker for NEM Dashboard Backend.

Runs as its own process (a separate Railway service) so NEMWEB ingestion
never competes with request serving in the API process.
"""
import asyncio
import logging
import os
import signal
from pathlib import Path

from app.data_ingester import build_data_ingester_from_env

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'info').upper(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_continuous_ingestion_until_stopped(data_ingester, update_interval: int):
    """Initialize the ingester and run continuous ingestion until a stop signal arrives.

    SIGTERM/SIGINT trigger stop_continuous_ingestion() then cancel the ingestion
    task, so Railway's graceful-stop signal drains cleanly before cleanup().
    """
    await data_ingester.initialize()

    ingestion_task = asyncio.create_task(
        data_ingester.run_continuous_ingestion(update_interval)
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_shutdown_signal():
        logger.info("Shutdown signal received, stopping continuous ingestion")
        data_ingester.stop_continuous_ingestion()
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_shutdown_signal)

    await stop_event.wait()

    ingestion_task.cancel()
    try:
        await ingestion_task
    except asyncio.CancelledError:
        pass

    await data_ingester.cleanup()
    logger.info("Ingestion worker stopped")


async def main():
    """Build the ingester from the shared env config and run it forever."""
    data_ingester = build_data_ingester_from_env()
    update_interval = int(os.getenv('UPDATE_INTERVAL_MINUTES', '5'))
    await run_continuous_ingestion_until_stopped(data_ingester, update_interval)


if __name__ == "__main__":
    from dotenv import load_dotenv

    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)

    logger.info("Starting NEM ingestion worker")
    asyncio.run(main())
