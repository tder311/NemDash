"""
Test fixtures for extended time range testing.
Generates realistic multi-day test data for aggregation tests.
"""
import pandas as pd
from datetime import datetime, timedelta
import random


def generate_dispatch_data(
    start_date: datetime,
    days: int,
    region: str = 'NSW',
    interval_minutes: int = 5
) -> pd.DataFrame:
    """Generate realistic dispatch data for extended range testing.

    Args:
        start_date: Start datetime
        days: Number of days of data
        region: NEM region
        interval_minutes: Data interval (default 5 minutes)

    Returns:
        DataFrame with dispatch data
    """
    records = []

    # Define DUIDs with their characteristics
    generators = [
        {'duid': 'BAYSW1', 'type': 'Coal', 'base_output': 550, 'region': 'NSW'},
        {'duid': 'BAYSW2', 'type': 'Coal', 'base_output': 550, 'region': 'NSW'},
        {'duid': 'ERGT01', 'type': 'Gas', 'base_output': 200, 'region': 'NSW'},
        {'duid': 'ARWF1', 'type': 'Wind', 'base_output': 100, 'region': 'NSW'},
        {'duid': 'BROKENH1', 'type': 'Solar', 'base_output': 50, 'region': 'NSW'},
    ]

    current = start_date
    end = start_date + timedelta(days=days)

    while current < end:
        hour = current.hour
        day_of_year = current.timetuple().tm_yday

        for gen in generators:
            if gen['region'] != region:
                continue

            base_output = gen['base_output']

            # Apply time-of-day variation
            if gen['type'] == 'Solar':
                # Solar follows sun: peaks at noon, zero at night
                if 6 <= hour <= 18:
                    solar_factor = 1 - abs(hour - 12) / 6
                else:
                    solar_factor = 0
                output = base_output * solar_factor
            elif gen['type'] == 'Wind':
                # Wind is variable but somewhat predictable
                wind_factor = 0.3 + 0.5 * abs((hour + day_of_year) % 24 - 12) / 12
                output = base_output * wind_factor
            else:
                # Baseload follows demand: higher during peak hours
                demand_factor = 0.7 + 0.3 * (1 - abs(hour - 18) / 12)
                output = base_output * demand_factor

            # Add some randomness
            output *= (0.9 + 0.2 * random.random())
            output = max(0, output)

            records.append({
                'settlementdate': current,
                'duid': gen['duid'],
                'scadavalue': round(output, 2),
                'uigf': 0.0,
                'totalcleared': round(output, 2),
                'ramprate': 0.0,
                'availability': base_output * 1.1,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            })

        current += timedelta(minutes=interval_minutes)

    return pd.DataFrame(records)


def generate_price_data(
    start_date: datetime,
    days: int,
    region: str = 'NSW',
    price_type: str = 'PUBLIC',
    interval_minutes: int = 5
) -> pd.DataFrame:
    """Generate realistic price data for extended range testing.

    Args:
        start_date: Start datetime
        days: Number of days of data
        region: NEM region
        price_type: Price type (DISPATCH, TRADING, PUBLIC)
        interval_minutes: Data interval

    Returns:
        DataFrame with price data
    """
    records = []
    current = start_date
    end = start_date + timedelta(days=days)

    while current < end:
        hour = current.hour
        day_of_week = current.weekday()

        # Base price with daily pattern (peak at 6-9pm)
        if 18 <= hour <= 21:
            base_price = 120  # Evening peak
        elif 7 <= hour <= 9:
            base_price = 100  # Morning peak
        elif 0 <= hour <= 5:
            base_price = 50   # Night valley
        else:
            base_price = 75   # Standard

        # Weekend discount
        if day_of_week >= 5:
            base_price *= 0.8

        # Add volatility
        price = base_price * (0.8 + 0.4 * random.random())

        # Demand follows similar pattern
        base_demand = 7000
        demand_factor = 0.7 + 0.3 * (1 - abs(hour - 18) / 12)
        demand = base_demand * demand_factor * (0.95 + 0.1 * random.random())

        records.append({
            'settlementdate': current,
            'region': region,
            'price': round(price, 2),
            'totaldemand': round(demand, 1),
            'price_type': price_type
        })

        current += timedelta(minutes=interval_minutes)

    return pd.DataFrame(records)


def generate_generator_info(region: str = 'NSW') -> list:
    """Generate generator info for test DUIDs.

    Args:
        region: NEM region

    Returns:
        List of generator info dicts
    """
    return [
        {
            'duid': 'BAYSW1',
            'station_name': 'Bayswater 1',
            'region': region,
            'fuel_source': 'Coal',
            'technology_type': 'Steam',
            'capacity_mw': 660
        },
        {
            'duid': 'BAYSW2',
            'station_name': 'Bayswater 2',
            'region': region,
            'fuel_source': 'Coal',
            'technology_type': 'Steam',
            'capacity_mw': 660
        },
        {
            'duid': 'ERGT01',
            'station_name': 'Eraring GT',
            'region': region,
            'fuel_source': 'Gas',
            'technology_type': 'OCGT',
            'capacity_mw': 240
        },
        {
            'duid': 'ARWF1',
            'station_name': 'Ararat Wind Farm',
            'region': region,
            'fuel_source': 'Wind',
            'technology_type': 'Wind',
            'capacity_mw': 120
        },
        {
            'duid': 'BROKENH1',
            'station_name': 'Broken Hill Solar',
            'region': region,
            'fuel_source': 'Solar',
            'technology_type': 'Solar PV',
            'capacity_mw': 60
        },
    ]
