import asyncio
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional
from pathlib import Path

import pandas as pd

from .nem_client import NEMDispatchClient
from .nem_price_client import NEMPriceClient
from .nem_pasa_client import NEMPASAClient
from .nem_predispatch_client import NEMPredispatchClient
from .nem_price_setter_client import NEMPriceSetterClient
from .nem_bid_client import NEMBidClient
from .database import NEMDatabase
from .forecaster import select_runs_at_leads
from .joint_inference import SHORT_LEAD_HOURS, fetch_bounds, fetch_terms, solve_unit_generation

NO_IC_FLOWS = pd.DataFrame(columns=["run_datetime", "interval_datetime", "interconnectorid", "mwflow"])
NO_REGION_DEMAND = pd.DataFrame(columns=["run_datetime", "interval_datetime", "regionid", "demand"])
# Buffer before the run's earliest interval, so bid bounds cover the run itself.
BOUNDS_LOOKBACK = timedelta(hours=1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def resolve_backfill_start(now: datetime) -> datetime:
    """Resolve the historical-backfill start date.

    BACKFILL_START_DATE (YYYY-MM-DD) or 365 days ago by default, then capped
    at the raw-retention window: backfilling past it would re-download data
    the daily retention sweep immediately deletes."""
    start_str = os.getenv('BACKFILL_START_DATE')
    start = None
    if start_str:
        try:
            start = datetime.strptime(start_str, '%Y-%m-%d')
        except ValueError:
            logger.warning(f"Invalid BACKFILL_START_DATE '{start_str}', using 365 days ago")
    if start is None:
        start = now - timedelta(days=365)

    retention_days = int(os.getenv('RAW_RETENTION_DAYS', '30'))
    if retention_days > 0:
        start = max(start, now - timedelta(days=retention_days))
    return start


def thin_pasa_for_multilead_backfill(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one run per (interval, region, lead bucket), ready for table insert.

    ``select_runs_at_leads`` adds ``lead_hours``/``lead_bucket`` columns used only
    for picking the nearest run per bucket; they aren't in the PASA table schema.
    """
    selected = select_runs_at_leads(df)
    return selected.drop(columns=["lead_hours", "lead_bucket"])


class DataIngester:
    def __init__(self, db_url: str, nem_base_url: str = "https://www.nemweb.com.au"):
        """Initialize the data ingester.

        Args:
            db_url: PostgreSQL database URL (e.g., 'postgresql://user:pass@localhost:5432/nem_dashboard')
            nem_base_url: Base URL for NEMWEB API
        """
        self.db = NEMDatabase(db_url)
        self.nem_client = NEMDispatchClient(nem_base_url)
        self.price_client = NEMPriceClient(nem_base_url)
        self.pasa_client = NEMPASAClient(nem_base_url)
        self.predispatch_client = NEMPredispatchClient(nem_base_url)
        self.price_setter_client = NEMPriceSetterClient(nem_base_url)
        self.bid_client = NEMBidClient(nem_base_url)
        self.is_running = False
        self._last_retention_day: Optional[date] = None

        # Track last fetched timestamps to avoid gaps
        # These are initialized from DB on startup in run_continuous_ingestion()
        self.last_dispatch_timestamp: Optional[datetime] = None
        self.last_dispatch_price_timestamp: Optional[datetime] = None
        self.last_trading_price_timestamp: Optional[datetime] = None
        self.last_pdpasa_run: Optional[datetime] = None
        self.last_stpasa_run: Optional[datetime] = None
        self.last_predispatch_run: Optional[datetime] = None
        self.pasa_ingestion_counter: int = 0  # Track cycles for PASA ingestion (every 6 cycles = 30 min)
        
    async def initialize(self):
        """Initialize the database"""
        await self.db.initialize()
        logger.info("Database initialized")
    
    async def ingest_current_data(self) -> bool:
        """Fetch and ingest new dispatch data and prices since last fetch.

        Only fetches files newer than the last successful fetch timestamp.
        PUBLIC prices are handled by background backfill, not here.
        """
        success = True

        try:
            # Fetch dispatch data since last fetch
            dispatch_df = await self.nem_client.get_all_current_dispatch_data(
                since=self.last_dispatch_timestamp
            )

            if dispatch_df is not None and not dispatch_df.empty:
                records_inserted = await self.db.insert_dispatch_data(dispatch_df)
                self.last_dispatch_timestamp = dispatch_df['settlementdate'].max()
                logger.info(f"Ingested {records_inserted} new dispatch records, latest: {self.last_dispatch_timestamp}")

            # Fetch dispatch prices since last fetch
            price_df = await self.price_client.get_all_current_dispatch_prices(
                since=self.last_dispatch_price_timestamp
            )

            if price_df is not None and not price_df.empty:
                price_records = await self.db.insert_price_data(price_df)
                self.last_dispatch_price_timestamp = price_df['settlementdate'].max()
                logger.info(f"Ingested {price_records} new dispatch price records, latest: {self.last_dispatch_price_timestamp}")

            # Fetch trading prices since last fetch
            trading_df = await self.price_client.get_all_current_trading_prices(
                since=self.last_trading_price_timestamp
            )

            if trading_df is not None and not trading_df.empty:
                trading_records = await self.db.insert_price_data(trading_df)
                self.last_trading_price_timestamp = trading_df['settlementdate'].max()
                logger.info(f"Ingested {trading_records} new trading price records, latest: {self.last_trading_price_timestamp}")

            # Recalculate daily metrics for recently completed days
            await self._recalculate_recent_metrics()

            return success

        except Exception as e:
            logger.error(f"Error ingesting current data: {e}")
            return False

    async def _recalculate_recent_metrics(self):
        """Recalculate daily metrics for yesterday and the day before (safety net for late data)."""
        from datetime import date
        REGIONS = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
        today = date.today()
        for days_back in [1, 2]:
            target_date = today - timedelta(days=days_back)
            for region in REGIONS:
                try:
                    await self.db.calculate_daily_metrics(region, target_date)
                except Exception as e:
                    logger.error(f"Error calculating metrics for {region} {target_date}: {e}")

        # Fetch bid data for yesterday and the day before
        for days_back in [1, 2]:
            target_date = today - timedelta(days=days_back)
            try:
                has_data = await self.db.has_bid_data_for_date(target_date)
                if not has_data:
                    result = await self.bid_client.get_daily_bids(
                        datetime.combine(target_date, datetime.min.time())
                    )
                    if result is not None:
                        day_df, per_df = result
                        if day_df is not None and not day_df.empty:
                            await self.db.insert_bid_day_offer(day_df)
                        if per_df is not None and not per_df.empty:
                            records = await self.db.insert_bid_per_offer(per_df)
                            logger.info(f"Ingested {records} bid records for {target_date}")
            except Exception as e:
                logger.error(f"Error fetching bid data for {target_date}: {e}")

        # Fetch price setter data for T-2 and T-3 (NEMDE archive has ~1-2 day delay)
        for days_back in [2, 3]:
            target_date = today - timedelta(days=days_back)
            try:
                df = await self.price_setter_client.get_daily_price_setter(
                    datetime.combine(target_date, datetime.min.time())
                )
                if df is not None and not df.empty:
                    await self.db.insert_price_setter_data(df)
                    for region in REGIONS:
                        try:
                            await self.db.calculate_daily_price_setter_metrics(region, target_date)
                        except Exception as e:
                            logger.error(f"Error calculating PS metrics {region} {target_date}: {e}")
            except Exception as e:
                logger.error(f"Error fetching price setter for {target_date}: {e}")

    async def backfill_daily_metrics(self, start_date, end_date=None) -> int:
        """Calculate daily metrics for all regions over a historical date range."""
        from datetime import date
        REGIONS = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']

        if end_date is None:
            end_date = date.today() - timedelta(days=1)

        current = start_date.date() if hasattr(start_date, 'date') else start_date
        end = end_date.date() if hasattr(end_date, 'date') else end_date

        total = 0
        while current <= end:
            for region in REGIONS:
                try:
                    success = await self.db.calculate_daily_metrics(region, current)
                    if success:
                        total += 1
                except Exception as e:
                    logger.error(f"Error backfilling metrics {region} {current}: {e}")
            current += timedelta(days=1)
            await asyncio.sleep(0.05)

        logger.info(f"Metrics backfill complete: {total} region-days calculated")
        return total

    async def backfill_price_setter_data(self, start_date, end_date=None) -> int:
        """Fetch NemPriceSetter data and calculate price setter metrics for a date range."""
        from datetime import date
        REGIONS = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']

        if end_date is None:
            end_date = date.today() - timedelta(days=2)  # Archive has ~2 day delay

        current = start_date.date() if hasattr(start_date, 'date') else start_date
        end = end_date.date() if hasattr(end_date, 'date') else end_date

        total_records = 0
        days_processed = 0
        while current <= end:
            try:
                df = await self.price_setter_client.get_daily_price_setter(
                    datetime.combine(current, datetime.min.time())
                )
                if df is not None and not df.empty:
                    records = await self.db.insert_price_setter_data(df)
                    total_records += records

                    for region in REGIONS:
                        try:
                            await self.db.calculate_daily_price_setter_metrics(region, current)
                        except Exception as e:
                            logger.error(f"Error calculating PS metrics {region} {current}: {e}")
            except Exception as e:
                logger.error(f"Error fetching price setter for {current}: {e}")

            days_processed += 1
            if days_processed % 10 == 0:
                logger.info(f"Price setter backfill progress: {days_processed} days, {total_records} records")

            current += timedelta(days=1)
            await asyncio.sleep(0.5)

        logger.info(f"Price setter backfill complete: {total_records} records over {days_processed} days")
        return total_records

    async def backfill_pasa_data(self, start_date, end_date=None,
                                 report_types=("PDPASA", "STPASA")) -> int:
        """Backfill historical PD/ST PASA from the NEMWEB archive over a window.

        Downloads the nested archive files overlapping the window, parses every
        run, thins to one run per (interval, region, lead bucket) across
        ``LEAD_BUCKETS`` (~5 leads spanning 12h-168h), and upserts into
        pdpasa_data / stpasa_data. ``report_types`` limits which PASA reports to
        pull (e.g. ("PDPASA",)).
        """
        if end_date is None:
            end_date = datetime.now()
        start = start_date.date() if hasattr(start_date, 'date') else start_date
        end = end_date.date() if hasattr(end_date, 'date') else end_date
        win_lo = pd.Timestamp(start)
        win_hi = pd.Timestamp(end) + pd.Timedelta(days=1)

        inserters = {"PDPASA": self.db.insert_pdpasa_data, "STPASA": self.db.insert_stpasa_data}
        total = 0
        for pasa_type in report_types:
            insert = inserters[pasa_type]
            try:
                files = await self.pasa_client.list_archive_files(pasa_type)
            except Exception as e:
                logger.error(f"Could not list {pasa_type} archive: {e}")
                continue

            # A file dated D holds runs forecasting intervals in ~[D, D+8], so a
            # window [start, end] needs files dated from ~start-8 up to end.
            wanted = [n for (n, d) in files
                      if (start - timedelta(days=8)) <= d.date() <= (end + timedelta(days=1))]
            logger.info(f"{pasa_type}: {len(wanted)} archive files cover {start}..{end}")

            for name in wanted:
                try:
                    df = await self.pasa_client.get_archive_pasa_file(pasa_type, name)
                    if df is None or df.empty:
                        continue
                    df = df[(df['interval_datetime'] >= win_lo) & (df['interval_datetime'] < win_hi)]
                    if df.empty:
                        continue
                    df = thin_pasa_for_multilead_backfill(df)  # ~1 run per (interval, region, lead bucket)
                    if not df.empty:
                        total += await insert(df)
                        logger.info(f"{pasa_type} {name}: +{len(df)} rows (running total {total})")
                except Exception as e:
                    logger.error(f"Error backfilling {pasa_type} file {name}: {e}")
                await asyncio.sleep(0.3)

        logger.info(f"PASA backfill complete: {total} rows over {start}..{end}")
        return total

    async def recalculate_price_setter_metrics(self, start_date, end_date=None) -> int:
        """Recalculate price setter daily metrics from existing raw data (no download)."""
        from datetime import date
        REGIONS = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']

        if end_date is None:
            end_date = date.today() - timedelta(days=1)

        current = start_date.date() if hasattr(start_date, 'date') else start_date
        end = end_date.date() if hasattr(end_date, 'date') else end_date

        total = 0
        while current <= end:
            for region in REGIONS:
                try:
                    success = await self.db.calculate_daily_price_setter_metrics(region, current)
                    if success:
                        total += 1
                except Exception as e:
                    logger.error(f"Error recalculating PS metrics {region} {current}: {e}")
            current += timedelta(days=1)
            await asyncio.sleep(0.05)

        logger.info(f"Price setter metrics recalculation complete: {total} region-days")
        return total

    async def backfill_bid_data(self, start_date, end_date=None) -> int:
        """Fetch and store bid data for a date range (all DUIDs)."""
        from datetime import date

        if end_date is None:
            end_date = date.today() - timedelta(days=1)

        current = start_date.date() if hasattr(start_date, 'date') else start_date
        end = end_date.date() if hasattr(end_date, 'date') else end_date

        total_records = 0
        days_processed = 0
        while current <= end:
            try:
                has_data = await self.db.has_bid_data_for_date(current)
                if not has_data:
                    result = await self.bid_client.get_daily_bids(
                        datetime.combine(current, datetime.min.time())
                    )
                    if result is not None:
                        day_df, per_df = result
                        if day_df is not None and not day_df.empty:
                            await self.db.insert_bid_day_offer(day_df)
                        if per_df is not None and not per_df.empty:
                            records = await self.db.insert_bid_per_offer(per_df)
                            total_records += records
            except Exception as e:
                logger.error(f"Error backfilling bids for {current}: {e}")

            days_processed += 1
            if days_processed % 10 == 0:
                logger.info(f"Bid backfill progress: {days_processed} days, {total_records} records")

            current += timedelta(days=1)
            await asyncio.sleep(1)

        logger.info(f"Bid backfill complete: {total_records} records over {days_processed} days")
        return total_records

    async def ingest_pasa_data(self) -> bool:
        """Fetch and ingest latest PASA (PDPASA and STPASA) data.

        PASA data is published every 30 minutes, so this should be called
        less frequently than dispatch/price data.
        """
        success = True

        try:
            # Fetch PDPASA
            pdpasa_df = await self.pasa_client.get_latest_pdpasa()

            if pdpasa_df is not None and not pdpasa_df.empty:
                # Check if we already have this run
                current_run = pdpasa_df['run_datetime'].iloc[0] if 'run_datetime' in pdpasa_df.columns else None

                if current_run is not None and (self.last_pdpasa_run is None or current_run > self.last_pdpasa_run):
                    records_inserted = await self.db.insert_pdpasa_data(pdpasa_df)
                    self.last_pdpasa_run = current_run
                    logger.info(f"Ingested {records_inserted} PDPASA records, run: {self.last_pdpasa_run}")
                else:
                    logger.debug(f"PDPASA data already ingested for run: {current_run}")

            # Fetch STPASA
            stpasa_df = await self.pasa_client.get_latest_stpasa()

            if stpasa_df is not None and not stpasa_df.empty:
                # Check if we already have this run
                current_run = stpasa_df['run_datetime'].iloc[0] if 'run_datetime' in stpasa_df.columns else None

                if current_run is not None and (self.last_stpasa_run is None or current_run > self.last_stpasa_run):
                    records_inserted = await self.db.insert_stpasa_data(stpasa_df)
                    self.last_stpasa_run = current_run
                    logger.info(f"Ingested {records_inserted} STPASA records, run: {self.last_stpasa_run}")
                else:
                    logger.debug(f"STPASA data already ingested for run: {current_run}")

            return success

        except Exception as e:
            logger.error(f"Error ingesting PASA data: {e}")
            return False

    async def ingest_predispatch_data(self) -> bool:
        """Fetch the latest pre-dispatch run and store RRP, interconnector, and constraint forecasts."""
        try:
            result = await self.predispatch_client.get_latest_predispatch_all()
            if result is None or result["prices"] is None or result["prices"].empty:
                return False
            df = result["prices"]
            current_run = df['run_datetime'].max()
            if self.last_predispatch_run is None or current_run > self.last_predispatch_run:
                inserted = await self.db.insert_predispatch_price(df)
                ic_df, con_df = result["interconnector"], result["constraint"]
                ic_inserted = await self.db.insert_predispatch_interconnector(ic_df) if ic_df is not None else 0
                con_inserted = await self.db.insert_predispatch_constraint(con_df) if con_df is not None else 0
                self.last_predispatch_run = current_run
                logger.info(
                    f"Ingested {inserted} price, {ic_inserted} interconnector, "
                    f"{con_inserted} constraint rows, run: {current_run}"
                )
                await self._infer_unit_generation(con_df, ic_df)
            else:
                logger.debug(f"Pre-dispatch already ingested for run: {current_run}")
            return True
        except Exception as e:
            logger.error(f"Error ingesting pre-dispatch data: {e}")
            return False

    async def _infer_unit_generation(self, con_df: Optional[pd.DataFrame], ic_df: Optional[pd.DataFrame]) -> None:
        """Backsolve unit MW from this run's UNFILTERED constraint frame and upsert good/weak rows.

        Uses con_df/ic_df straight from the parser, not the binding-only predispatch_constraint
        table, since the solver needs every constraint's lhs. Never raises -- a solve failure
        here must not break price/network ingestion, which is why it's called after that commits.
        """
        if con_df is None or con_df.empty:
            return
        try:
            lhs_frame = con_df[["run_datetime", "interval_datetime", "constraintid", "lhs"]]
            bounds_start = con_df["interval_datetime"].min() - BOUNDS_LOOKBACK
            bounds_end = con_df["interval_datetime"].max()
            run_date = con_df["run_datetime"].max()
            terms = await fetch_terms(self.db, run_date)
            bounds = await fetch_bounds(self.db, bounds_start, bounds_end)

            solved = solve_unit_generation(
                lhs_frame, terms, ic_df if ic_df is not None else NO_IC_FLOWS, NO_REGION_DEMAND, bounds=bounds,
            )
            if solved.empty:
                logger.info("Joint unit inference: no solvable (run, interval) systems in this run")
                return
            persisted = await self.db.insert_inferred_unit_generation(solved)
            lead = solved["interval_datetime"] - solved["run_datetime"]
            n_short_lead = int((lead <= pd.Timedelta(hours=SHORT_LEAD_HOURS)).sum())
            logger.info(
                f"Joint unit inference: solved {len(solved)} rows across "
                f"{solved['duid'].nunique()} DUIDs ({n_short_lead} within {SHORT_LEAD_HOURS:.0f}h lead), "
                f"persisted {persisted} good/weak rows"
            )
        except Exception as e:
            logger.error(f"Error running joint unit inference: {e}")

    async def backfill_predispatch_data(self, start_date, end_date=None) -> int:
        """Backfill historical pre-dispatch price (RRP) from the NEMWEB archive."""
        if end_date is None:
            end_date = datetime.now()
        start = start_date.date() if hasattr(start_date, 'date') else start_date
        end = end_date.date() if hasattr(end_date, 'date') else end_date
        win_lo = pd.Timestamp(start)
        win_hi = pd.Timestamp(end) + pd.Timedelta(days=1)

        try:
            files = await self.predispatch_client.list_archive_files()
        except Exception as e:
            logger.error(f"Could not list pre-dispatch archive: {e}")
            return 0

        wanted = [n for (n, d) in files
                  if (start - timedelta(days=2)) <= d.date() <= (end + timedelta(days=1))]
        logger.info(f"Pre-dispatch backfill: {len(wanted)} archive files cover {start}..{end}")

        total = 0
        for name in wanted:
            try:
                df = await self.predispatch_client.get_archive_predispatch_file(name)
                if df is None or df.empty:
                    continue
                df = df[(df['interval_datetime'] >= win_lo) & (df['interval_datetime'] < win_hi)]
                if not df.empty:
                    total += await self.db.insert_predispatch_price(df)
                    logger.info(f"Pre-dispatch {name}: +{len(df)} rows (total {total})")
            except Exception as e:
                logger.error(f"Error backfilling pre-dispatch {name}: {e}")
            await asyncio.sleep(0.3)

        logger.info(f"Pre-dispatch backfill complete: {total} rows over {start}..{end}")
        return total

    async def ingest_historical_data(self, start_date: datetime, end_date: Optional[datetime] = None) -> int:
        """Fetch and ingest historical dispatch data for a date range"""
        if end_date is None:
            end_date = start_date
            
        total_records = 0
        current_date = start_date
        
        while current_date <= end_date:
            try:
                logger.info(f"Fetching historical data for {current_date.strftime('%Y-%m-%d')}")
                df = await self.nem_client.get_historical_dispatch_data(current_date)
                
                if df is not None and not df.empty:
                    records_inserted = await self.db.insert_dispatch_data(df)
                    total_records += records_inserted
                    logger.info(f"Inserted {records_inserted} records for {current_date.strftime('%Y-%m-%d')}")
                else:
                    logger.warning(f"No data available for {current_date.strftime('%Y-%m-%d')}")
                
                # Small delay to avoid overwhelming the API
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error ingesting data for {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Historical ingestion complete. Total records: {total_records}")
        return total_records
    
    async def ingest_historical_prices(self, start_date: datetime, end_date: Optional[datetime] = None) -> int:
        """Fetch and ingest historical price data (PUBLIC_PRICES) for a date range"""
        if end_date is None:
            end_date = start_date

        total_records = 0
        current_date = start_date

        while current_date <= end_date:
            try:
                logger.info(f"Fetching historical prices for {current_date.strftime('%Y-%m-%d')}")
                price_df = await self.price_client.get_daily_prices(current_date)

                if price_df is not None and not price_df.empty:
                    records_inserted = await self.db.insert_price_data(price_df)
                    total_records += records_inserted
                    logger.info(f"Inserted {records_inserted} price records for {current_date.strftime('%Y-%m-%d')}")
                else:
                    logger.warning(f"No price data available for {current_date.strftime('%Y-%m-%d')}")

                # Small delay to avoid overwhelming the API
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error ingesting price data for {current_date}: {e}")

            current_date += timedelta(days=1)

        logger.info(f"Historical price ingestion complete. Total records: {total_records}")
        return total_records

    async def backfill_missing_data(self, start_date: datetime) -> int:
        """Backfill missing historical price data (runs in background).

        Fetches entire months at once from Archive for efficiency,
        instead of downloading the same monthly ZIP file for each day.

        Args:
            start_date: Backfill all data from this date onwards
        """
        logger.info(f"Starting automatic backfill check from {start_date.strftime('%Y-%m-%d')}...")

        try:
            end_date = datetime.now()

            # Find missing dates
            missing_dates = await self.db.get_missing_dates(start_date, end_date, price_type='PUBLIC')

            if not missing_dates:
                logger.info("No missing dates found - data is complete")
                return 0

            logger.info(f"Found {len(missing_dates)} missing dates since {start_date.strftime('%Y-%m-%d')}")

            # Group missing dates by month for efficient fetching
            months_with_missing = {}
            for date in missing_dates:
                month_key = (date.year, date.month)
                if month_key not in months_with_missing:
                    months_with_missing[month_key] = []
                months_with_missing[month_key].append(date)

            logger.info(f"Missing data spans {len(months_with_missing)} months")

            total_records = 0
            months_processed = 0
            total_months = len(months_with_missing)

            for (year, month), dates in sorted(months_with_missing.items()):
                try:
                    # Try the monthly Archive ZIP first (one request per month).
                    price_df = await self.price_client.get_monthly_archive_prices(year, month)
                    if price_df is not None and not price_df.empty:
                        records = await self.db.insert_price_data(price_df)
                        total_records += records
                        logger.info(f"Backfilled {year}-{month:02d} from Archive: {records} records")
                    else:
                        # Archive not yet published (recent months) or 404 — fall back
                        # to per-day fetches from the Current directory, which retains
                        # ~60 days of daily files.
                        logger.info(f"Archive unavailable for {year}-{month:02d}, falling back to Current directory ({len(dates)} days)")
                        month_records = 0
                        for date in dates:
                            price_df = await self.price_client.get_daily_prices(date)
                            if price_df is not None and not price_df.empty:
                                records = await self.db.insert_price_data(price_df)
                                month_records += records
                                total_records += records
                            await asyncio.sleep(0.5)
                        logger.info(f"Backfilled {year}-{month:02d} from Current: {month_records} records")

                    months_processed += 1
                    logger.info(f"Price backfill progress: {months_processed}/{total_months} months ({total_records} records)")

                    # Delay between months to respect NEMWEB rate limits
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"Error backfilling {year}-{month:02d}: {e}")
                    continue

            logger.info(f"Price backfill complete. Total records added: {total_records}")
            return total_records

        except Exception as e:
            logger.error(f"Error during backfill: {e}")
            return 0

    async def backfill_dispatch_prices(self) -> int:
        """Backfill DISPATCH prices from Current directory since last PUBLIC price.

        Only fetches files newer than the latest PUBLIC price timestamp,
        reducing the number of files from ~288 (3 days) to typically ~100-200
        (since 4am today). Uses concurrent requests for faster fetching.
        """
        logger.info("Starting DISPATCH price backfill from Current directory...")

        try:
            # Get the latest PUBLIC price timestamp to avoid fetching unnecessary data
            latest_public = await self.db.get_latest_price_timestamp('PUBLIC')

            if latest_public:
                logger.info(f"Latest PUBLIC price: {latest_public}, fetching DISPATCH since then")
            else:
                logger.info("No PUBLIC prices found, fetching all available DISPATCH data")

            # Fetch only files newer than latest PUBLIC timestamp
            df = await self.price_client.get_all_current_dispatch_prices(since=latest_public)

            if df is not None and not df.empty:
                records = await self.db.insert_price_data(df)
                logger.info(f"Backfilled {records} DISPATCH price records from Current directory")
                return records
            else:
                logger.info("No new DISPATCH price data to backfill")
                return 0

        except Exception as e:
            logger.error(f"Error during DISPATCH price backfill: {e}")
            return 0

    async def backfill_dispatch_data(self, start_date: datetime) -> int:
        """Backfill dispatch SCADA data from historical archives.

        Note: Recent data from Current directory (~3 days) should already be loaded
        before calling this method. This only fetches older data from archives.

        Args:
            start_date: Backfill all dispatch data from this date onwards
        """
        total_records = 0

        try:
            # Backfill older data from historical archives
            # Archives are available with ~2 day delay, so we can fill gaps older than ~3 days
            logger.info(f"Checking for missing dispatch data since {start_date.strftime('%Y-%m-%d')}...")

            end_date = datetime.now()

            # Find dates that need backfilling from archives
            missing_dates = await self._get_missing_dispatch_dates(start_date, end_date)

            if missing_dates:
                logger.info(f"Found {len(missing_dates)} dates to backfill from archives")

                for date in missing_dates:
                    try:
                        logger.info(f"Backfilling dispatch data for {date.strftime('%Y-%m-%d')} from archive...")
                        archive_df = await self.nem_client.get_historical_dispatch_data(date)

                        if archive_df is not None and not archive_df.empty:
                            records = await self.db.insert_dispatch_data(archive_df)
                            total_records += records
                            logger.info(f"Backfilled {records} dispatch records for {date.strftime('%Y-%m-%d')}")

                        # Delay between requests to respect NEMWEB rate limits
                        await asyncio.sleep(1)

                    except Exception as e:
                        logger.warning(f"Could not backfill dispatch for {date.strftime('%Y-%m-%d')}: {e}")
                        continue
            else:
                logger.info("No missing dispatch dates found - data is complete")

            logger.info(f"Dispatch backfill complete. Total records added: {total_records}")
            return total_records

        except Exception as e:
            logger.error(f"Error during dispatch data backfill: {e}")
            return total_records

    async def _get_missing_dispatch_dates(self, start_date: datetime, end_date: datetime) -> list:
        """Find dates with no or incomplete dispatch data in the specified range.

        Archives have ~2 day delay, so we only check dates at least 2 days old.
        """
        # Archives are posted with ~2 day delay
        archive_cutoff = datetime.now() - timedelta(days=2)

        # Only check dates that would be available in archives
        check_end = min(end_date, archive_cutoff)

        if start_date >= check_end:
            return []

        # Get dates that have sufficient dispatch data
        existing_dates = await self.db.get_dispatch_dates_with_data(start_date, check_end)

        # Generate all dates in range and find missing ones
        missing = []
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while current <= check_end:
            date_str = current.strftime('%Y-%m-%d')
            if date_str not in existing_dates:
                missing.append(current)
            current += timedelta(days=1)

        return missing

    async def _run_historical_backfill(self, start_date: datetime):
        """Run historical backfill in background. Called after site is already usable."""
        logger.info(f"Starting background historical backfill from {start_date.strftime('%Y-%m-%d')}...")

        try:
            # Backfill missing historical PUBLIC price data
            await self.backfill_missing_data(start_date=start_date)

            # Backfill dispatch SCADA data from archives (older than ~3 days)
            await self.backfill_dispatch_data(start_date=start_date)

            # Backfill daily metrics from earliest available data (not limited to BACKFILL_START_DATE)
            logger.info("Starting daily metrics backfill...")
            metrics_start = await self.db.get_earliest_metrics_date()
            if metrics_start:
                await self.backfill_daily_metrics(start_date=metrics_start)
            else:
                logger.info("No overlapping dispatch+price data found for metrics backfill")

            # Backfill price setter data from NEMDE archive
            logger.info("Starting price setter backfill...")
            await self.backfill_price_setter_data(start_date=start_date)

            # Backfill bid data
            logger.info("Starting bid data backfill...")
            await self.backfill_bid_data(start_date=start_date)

            logger.info("Background historical backfill complete")
        except Exception as e:
            logger.error(f"Error in background backfill: {e}")

    async def run_continuous_ingestion(self, interval_minutes: int = 5):
        """Run continuous data ingestion"""
        self.is_running = True
        logger.info(f"Starting continuous ingestion with {interval_minutes} minute intervals")

        backfill_start_date = resolve_backfill_start(datetime.now())
        logger.info(f"Backfill start date: {backfill_start_date.strftime('%Y-%m-%d')}")

        # FIRST: Initialize timestamps from database to avoid re-fetching existing data
        self.last_dispatch_timestamp = await self.db.get_latest_dispatch_timestamp()
        self.last_dispatch_price_timestamp = await self.db.get_latest_price_timestamp('DISPATCH')
        self.last_trading_price_timestamp = await self.db.get_latest_price_timestamp('TRADING')
        self.last_pdpasa_run = await self.db.get_latest_pdpasa_run_datetime()
        self.last_stpasa_run = await self.db.get_latest_stpasa_run_datetime()

        logger.info(f"Existing data timestamps - dispatch: {self.last_dispatch_timestamp}, "
                    f"dispatch_price: {self.last_dispatch_price_timestamp}, "
                    f"trading_price: {self.last_trading_price_timestamp}, "
                    f"pdpasa: {self.last_pdpasa_run}, stpasa: {self.last_stpasa_run}")

        # Fetch recent data from Current directories (only files newer than existing data)
        logger.info("Fetching new data from Current directories...")

        # Fetch dispatch SCADA data (only new files)
        dispatch_df = await self.nem_client.get_all_current_dispatch_data(since=self.last_dispatch_timestamp)
        if dispatch_df is not None and not dispatch_df.empty:
            records = await self.db.insert_dispatch_data(dispatch_df)
            self.last_dispatch_timestamp = dispatch_df['settlementdate'].max()
            logger.info(f"Loaded {records} new dispatch records from Current directory")
        else:
            logger.info("No new dispatch data to fetch")

        # Fetch DISPATCH prices (only new files)
        dispatch_price_df = await self.price_client.get_all_current_dispatch_prices(since=self.last_dispatch_price_timestamp)
        if dispatch_price_df is not None and not dispatch_price_df.empty:
            records = await self.db.insert_price_data(dispatch_price_df)
            self.last_dispatch_price_timestamp = dispatch_price_df['settlementdate'].max()
            logger.info(f"Loaded {records} new dispatch price records from Current directory")
        else:
            logger.info("No new dispatch price data to fetch")

        # Fetch TRADING prices (only new files)
        trading_df = await self.price_client.get_all_current_trading_prices(since=self.last_trading_price_timestamp)
        if trading_df is not None and not trading_df.empty:
            records = await self.db.insert_price_data(trading_df)
            self.last_trading_price_timestamp = trading_df['settlementdate'].max()
            logger.info(f"Loaded {records} new trading price records from Current directory")
        else:
            logger.info("No new trading price data to fetch")

        # Fetch today's and yesterday's PUBLIC prices for immediate use (if missing)
        today = datetime.now()
        yesterday = today - timedelta(days=1)

        yesterday_df = await self.price_client.get_daily_prices(yesterday)
        if yesterday_df is not None and not yesterday_df.empty:
            records = await self.db.insert_price_data(yesterday_df)
            logger.info(f"Loaded {records} PUBLIC price records for yesterday")

        today_df = await self.price_client.get_daily_prices(today)
        if today_df is not None and not today_df.empty:
            records = await self.db.insert_price_data(today_df)
            logger.info(f"Loaded {records} PUBLIC price records for today")

        # Fetch PASA data for immediate use
        logger.info("Fetching PASA data...")
        await self.ingest_pasa_data()
        await self.ingest_predispatch_data()

        logger.info("Site is now usable with recent data. Starting background historical backfill...")

        # PRIORITY 2: Start historical backfill in background
        # This runs concurrently with the main ingestion loop
        _backfill_task = asyncio.create_task(self._run_historical_backfill(backfill_start_date))  # noqa: F841

        # Initial data fetch (will fetch files newer than the timestamps above)
        await self.ingest_current_data()

        while self.is_running:
            try:
                await asyncio.sleep(interval_minutes * 60)
                if self.is_running:
                    await self.ingest_current_data()

                    # Ingest PASA data every 6 cycles (~30 minutes when interval=5 min)
                    self.pasa_ingestion_counter += 1
                    if self.pasa_ingestion_counter >= 6:
                        await self.ingest_pasa_data()
                        await self.ingest_predispatch_data()
                        self.pasa_ingestion_counter = 0

                    await self._maybe_apply_retention()
            except Exception as e:
                logger.error(f"Error in continuous ingestion: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    async def _maybe_apply_retention(self):
        """Trim raw dispatch/bid rows to RAW_RETENTION_DAYS, once per calendar day.

        Set RAW_RETENTION_DAYS=0 to disable."""
        days = int(os.getenv('RAW_RETENTION_DAYS', '30'))
        if days <= 0 or self._last_retention_day == date.today():
            return
        deleted = await self.db.apply_raw_retention(days=days)
        self._last_retention_day = date.today()
        if any(deleted.values()):
            logger.info(f"Raw retention ({days}d): deleted {deleted}")

    def stop_continuous_ingestion(self):
        """Stop continuous data ingestion"""
        self.is_running = False
        logger.info("Stopping continuous ingestion")
    
    async def get_data_summary(self):
        """Get summary of ingested data"""
        return await self.db.get_data_summary()
    
    async def cleanup(self):
        """Clean up resources"""
        logger.info("Data ingester cleaned up")

# Sample generator information for common NEM units
SAMPLE_GENERATOR_INFO = [
    {"duid": "ADPCC1", "station_name": "Adelaide Desalination Plant", "region": "SA", "fuel_source": "Solar", "technology_type": "Solar PV", "capacity_mw": 1.2},
    {"duid": "AGLHAL", "station_name": "Hallett Wind Farm", "region": "SA", "fuel_source": "Wind", "technology_type": "Wind", "capacity_mw": 94.5},
    {"duid": "AGLSOM", "station_name": "AGL Somerton", "region": "VIC", "fuel_source": "Gas", "technology_type": "Gas Turbine", "capacity_mw": 160},
    {"duid": "ANGASG1", "station_name": "Angaston Gas", "region": "SA", "fuel_source": "Gas", "technology_type": "Gas Turbine", "capacity_mw": 50},
    {"duid": "APD01", "station_name": "Port Stanvac", "region": "SA", "fuel_source": "Diesel", "technology_type": "Reciprocating Engine", "capacity_mw": 56},
    {"duid": "ARWF1", "station_name": "Ararat Wind Farm", "region": "VIC", "fuel_source": "Wind", "technology_type": "Wind", "capacity_mw": 240},
    {"duid": "BALBG1", "station_name": "Ballarat Base Hospital", "region": "VIC", "fuel_source": "Gas", "technology_type": "Gas Turbine", "capacity_mw": 1.0},
    {"duid": "BARRON1", "station_name": "Barron Gorge", "region": "QLD", "fuel_source": "Hydro", "technology_type": "Hydro", "capacity_mw": 66},
    {"duid": "BASTYAN", "station_name": "Bastyan", "region": "TAS", "fuel_source": "Hydro", "technology_type": "Hydro", "capacity_mw": 82},
    {"duid": "BBTHREE1", "station_name": "BB1 Unit 1", "region": "NSW", "fuel_source": "Coal", "technology_type": "Steam Turbine", "capacity_mw": 350}
]

async def update_sample_generator_info(db: NEMDatabase):
    """Update database with sample generator information"""
    await db.update_generator_info(SAMPLE_GENERATOR_INFO)
    logger.info(f"Updated {len(SAMPLE_GENERATOR_INFO)} generator info records")


async def import_generator_info_from_csv(db: NEMDatabase, csv_path: str = None):
    """
    Import generator info from GenInfo.csv if available.
    Falls back to sample generator info if CSV not found.
    """
    # Try to find GenInfo.csv in common locations
    if csv_path is None:
        possible_paths = [
            Path(__file__).parent.parent / 'data' / 'GenInfo.csv',
            Path('./data/GenInfo.csv'),
        ]
        for path in possible_paths:
            if path.exists():
                csv_path = str(path)
                break

    if csv_path is None or not Path(csv_path).exists():
        logger.warning("GenInfo.csv not found, using sample generator info only")
        await update_sample_generator_info(db)
        return

    logger.info(f"Importing generator info from {csv_path}")

    try:
        # Read the CSV file
        df = pd.read_csv(csv_path, encoding='utf-8-sig')  # Handle BOM
        df.columns = df.columns.str.strip()  # Clean column names

        # Filter for existing plants with DUIDs
        df_valid = df[
            (df['DUID'].notna()) &
            (df['DUID'] != '') &
            (df['Asset Type'].str.contains('Existing', na=False))
        ].copy()

        logger.info(f"Found {len(df_valid)} existing generators with DUIDs in CSV")

        # Process generators
        generators = []
        for _, row in df_valid.iterrows():
            duid = str(row['DUID']).strip()
            if not duid or duid == 'nan':
                continue

            # Map region (remove the '1' suffix)
            region = str(row['Region']).replace('1', '').strip()

            # Clean fuel type mapping
            fuel_type_raw = str(row['Fuel Type']).strip()
            if 'Solar' in fuel_type_raw:
                fuel_source = 'Solar'
            elif 'Wind' in fuel_type_raw:
                fuel_source = 'Wind'
            elif 'Water' in fuel_type_raw or 'Hydro' in fuel_type_raw:
                fuel_source = 'Hydro'
            elif 'Gas' in fuel_type_raw or 'Coal Mine Gas' in fuel_type_raw:
                fuel_source = 'Gas'
            elif 'Coal' in fuel_type_raw:
                fuel_source = 'Coal'
            elif 'Other' in fuel_type_raw and 'Battery' in str(row.get('Technology Type', '')):
                fuel_source = 'Battery'
            elif 'Diesel' in fuel_type_raw:
                fuel_source = 'Diesel'
            else:
                fuel_source = 'Other'

            # Clean technology type
            tech_type_raw = str(row.get('Technology Type', '')).strip()
            if 'Solar PV' in tech_type_raw:
                technology_type = 'Solar PV'
            elif 'Wind Turbine' in tech_type_raw:
                technology_type = 'Wind'
            elif 'Storage - Battery' in tech_type_raw:
                technology_type = 'Battery Storage'
            elif 'Hydro' in tech_type_raw:
                technology_type = 'Hydro'
            elif 'Gas Turbine' in tech_type_raw:
                technology_type = 'Gas Turbine'
            elif 'Steam Turbine' in tech_type_raw:
                if fuel_source == 'Coal':
                    technology_type = 'Coal Steam'
                else:
                    technology_type = 'Gas Steam'
            elif 'Reciprocating Engine' in tech_type_raw:
                technology_type = 'Reciprocating Engine'
            else:
                technology_type = tech_type_raw if tech_type_raw else 'Unknown'

            # Get capacity (try different columns)
            capacity = 0.0
            for cap_col in ['Nameplate Capacity (MW)', 'Aggregated Upper Nameplate Capacity (MW)', 'Upper Nameplate Capacity (MW)']:
                if cap_col in row and pd.notna(row[cap_col]):
                    try:
                        cap_str = str(row[cap_col]).strip().replace(' - ', '-')
                        if '-' in cap_str:
                            # Handle range like "200.00 - 400.00"
                            cap_parts = cap_str.split('-')
                            capacity = float(cap_parts[-1].strip())
                        else:
                            capacity = float(cap_str)
                        break
                    except (ValueError, IndexError):
                        continue

            if capacity == 0.0:
                capacity = 100.0  # Default

            # Clean site name
            station_name = str(row.get('Site Name', duid)).strip()
            if not station_name or station_name == 'nan':
                station_name = duid

            generators.append({
                'duid': duid,
                'station_name': station_name,
                'region': region,
                'fuel_source': fuel_source,
                'technology_type': technology_type,
                'capacity_mw': capacity
            })

        if generators:
            await db.update_generator_info(generators)
            logger.info(f"Imported {len(generators)} generator records from GenInfo.csv")
        else:
            logger.warning("No valid generators found in CSV, using sample data")
            await update_sample_generator_info(db)

    except Exception as e:
        logger.error(f"Error importing GenInfo.csv: {e}, falling back to sample data")
        await update_sample_generator_info(db)