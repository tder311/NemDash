"""Train the NEM price forecaster against the populated Postgres database.

Run from the backend directory (``nem-dashboard-backend/``):

    python -m scripts.train_forecaster --days 365

Reads ``DATABASE_URL`` from the environment (.env). Pulls realised 30-min
TRADING prices joined to their leakage-safe PASA forecast features, runs a
walk-forward backtest, trains the final model on the whole window, and writes
it to ``FORECAST_MODEL_PATH`` (default ``app/../models/price_forecaster.joblib``).
The running API picks the new model up on next load.
"""

import argparse
import asyncio
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

from app.database import NEMDatabase
from app.forecaster import (
    TARGET,
    PriceForecaster,
    assemble_features,
    default_model_path,
    load_training_frame,
    walk_forward_validate,
)


async def train(days: int, out: str) -> None:
    load_dotenv()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set (check your .env).")

    db = NEMDatabase(db_url)
    await db.initialize()
    try:
        end = datetime.now()
        start = end - timedelta(days=days)
        print(f"Loading training data {start:%Y-%m-%d} .. {end:%Y-%m-%d} ...")
        merged = await load_training_frame(db, start, end)
        if merged.empty:
            print(
                "No training data in that window. Has the DB been backfilled "
                "with both TRADING prices and PASA history?"
            )
            return

        X, y, names = assemble_features(merged)
        # Rebuild the interval order with the same non-null-price mask used
        # inside assemble_features, so it aligns row-for-row with X / y.
        order = merged.loc[merged[TARGET].notna(), "interval_datetime"].reset_index(drop=True)
        print(
            f"Assembled {len(X):,} rows x {len(names)} features across "
            f"{merged['region'].nunique()} region(s)."
        )

        print("Walk-forward validation ...")
        val = walk_forward_validate(X, y, order)
        print(
            f"  MAE={val['mae']:.2f}  RMSE={val['rmse']:.2f}  "
            f"Spearman={val.get('spearman', float('nan')):.3f}  ({len(val['folds'])} folds)"
        )
        print(
            f"  SpikeRecall={val.get('spike_recall', float('nan')):.3f}  "
            f"PinballP10={val.get('pinball_p10', float('nan')):.2f}  "
            f"PinballP90={val.get('pinball_p90', float('nan')):.2f}"
        )

        print("Training final model on the full window ...")
        model = PriceForecaster().train(X, y)
        model.card.metrics = val

        os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
        model.save(out)
        print(f"Saved model -> {out}")

        print("Top features by importance:")
        for name, imp in model.feature_importance(12):
            print(f"  {name:<30} {imp:.3f}")
    finally:
        await db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Train the NEM 30-min price forecaster.")
    ap.add_argument("--days", type=int, default=365, help="training window length in days")
    ap.add_argument("--out", default=default_model_path(), help="model output path")
    args = ap.parse_args()
    asyncio.run(train(args.days, args.out))


if __name__ == "__main__":
    main()
