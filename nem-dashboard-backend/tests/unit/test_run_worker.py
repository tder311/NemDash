"""
Unit tests for run_worker.py — the standalone continuous-ingestion process.

Uses a fully mocked DataIngester so no network or database access occurs.
"""
import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock

import pytest

import run_worker


def make_mock_ingester():
    ingester = MagicMock()
    ingester.initialize = AsyncMock()
    ingester.run_continuous_ingestion = AsyncMock()
    ingester.stop_continuous_ingestion = MagicMock()
    ingester.cleanup = AsyncMock()
    return ingester


class TestRunContinuousIngestionUntilStopped:
    """Tests for the worker's core loop-wiring coroutine."""

    @pytest.mark.asyncio
    async def test_wires_initialize_and_run_continuous_ingestion(self, monkeypatch):
        """The coroutine initializes the ingester and starts continuous ingestion."""
        mock_ingester = make_mock_ingester()
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "add_signal_handler", lambda sig, cb: None)

        worker_task = asyncio.create_task(
            run_worker.run_continuous_ingestion_until_stopped(mock_ingester, update_interval=5)
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        mock_ingester.initialize.assert_awaited_once()
        mock_ingester.run_continuous_ingestion.assert_called_once_with(5)

        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_signal_handler_stops_ingestion_and_cleans_up(self, monkeypatch):
        """A registered SIGTERM/SIGINT handler stops ingestion and cleans up."""
        mock_ingester = make_mock_ingester()
        captured_handlers = {}
        loop = asyncio.get_running_loop()

        def fake_add_signal_handler(sig, callback):
            captured_handlers[sig] = callback

        monkeypatch.setattr(loop, "add_signal_handler", fake_add_signal_handler)

        worker_task = asyncio.create_task(
            run_worker.run_continuous_ingestion_until_stopped(mock_ingester, update_interval=5)
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert signal.SIGTERM in captured_handlers
        assert signal.SIGINT in captured_handlers

        captured_handlers[signal.SIGTERM]()

        await asyncio.wait_for(worker_task, timeout=1)

        mock_ingester.stop_continuous_ingestion.assert_called_once()
        mock_ingester.cleanup.assert_awaited_once()


class TestMain:
    """Tests for main() — builds the ingester from env, then runs the loop."""

    @pytest.mark.asyncio
    async def test_main_builds_ingester_and_runs_loop(self, monkeypatch):
        """main() builds the ingester via the shared env helper and runs the loop."""
        mock_ingester = make_mock_ingester()
        monkeypatch.setattr(
            run_worker, "build_data_ingester_from_env", MagicMock(return_value=mock_ingester)
        )
        monkeypatch.setenv("UPDATE_INTERVAL_MINUTES", "7")
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(loop, "add_signal_handler", lambda sig, cb: None)

        main_task = asyncio.create_task(run_worker.main())
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        run_worker.build_data_ingester_from_env.assert_called_once()
        mock_ingester.initialize.assert_awaited_once()
        mock_ingester.run_continuous_ingestion.assert_called_once_with(7)

        main_task.cancel()
        try:
            await main_task
        except asyncio.CancelledError:
            pass
