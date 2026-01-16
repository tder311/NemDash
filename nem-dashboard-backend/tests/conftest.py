"""
Shared pytest fixtures for NEM Dashboard tests
"""
import pytest
import tempfile
from pathlib import Path
from datetime import datetime
import pandas as pd
import sys
import os

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.database import NEMDatabase
from app.nem_client import NEMDispatchClient
from app.nem_price_client import NEMPriceClient

# Import fixtures
from tests.fixtures.sample_dispatch_csv import (
    SAMPLE_DISPATCH_CSV,
    SAMPLE_DIRECTORY_HTML,
    create_sample_dispatch_zip,
    create_empty_zip,
)
from tests.fixtures.sample_price_csv import (
    SAMPLE_DISPATCH_PRICE_CSV,
    SAMPLE_TRADING_PRICE_CSV,
    SAMPLE_PUBLIC_PRICE_CSV,
    create_price_zip,
)
from tests.fixtures.sample_interconnector_csv import (
    SAMPLE_INTERCONNECTOR_CSV,
    create_interconnector_zip,
)


# ============================================================================
# Client Fixtures
# ============================================================================

@pytest.fixture
def nem_client():
    """Create NEMDispatchClient instance"""
    return NEMDispatchClient("https://www.nemweb.com.au")


@pytest.fixture
def price_client():
    """Create NEMPriceClient instance"""
    return NEMPriceClient("https://www.nemweb.com.au")


# ============================================================================
# Database Fixtures
# ============================================================================

@pytest.fixture
async def test_db():
    """Create a temporary database for testing"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    db = NEMDatabase(db_path)
    await db.initialize()
    yield db

    # Cleanup
    try:
        Path(db_path).unlink(missing_ok=True)
    except Exception:
        pass


@pytest.fixture
async def populated_db(test_db):
    """Database with sample data pre-loaded"""
    # Insert sample dispatch data
    dispatch_df = pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'duid': 'BAYSW1',
            'scadavalue': 350.5,
            'uigf': 0.0,
            'totalcleared': 350.0,
            'ramprate': 0.0,
            'availability': 400.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'duid': 'AGLHAL',
            'scadavalue': 94.2,
            'uigf': 0.0,
            'totalcleared': 94.0,
            'ramprate': 0.0,
            'availability': 95.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 35),
            'duid': 'ARWF1',
            'scadavalue': 185.0,
            'uigf': 0.0,
            'totalcleared': 185.0,
            'ramprate': 0.0,
            'availability': 200.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        },
    ])
    await test_db.insert_dispatch_data(dispatch_df)

    # Insert sample price data
    price_df = pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'VIC',
            'price': 72.30,
            'totaldemand': 5200.0,
            'price_type': 'DISPATCH'
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 90.00,
            'totaldemand': 7500.0,
            'price_type': 'TRADING'
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 88.00,
            'totaldemand': 7400.0,
            'price_type': 'PUBLIC'
        },
        {
            'settlementdate': datetime(2025, 1, 12, 12, 0),
            'region': 'NSW',
            'price': 75.00,
            'totaldemand': 7200.0,
            'price_type': 'PUBLIC'
        },
    ])
    await test_db.insert_price_data(price_df)

    # Insert sample interconnector data
    interconnector_df = pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'interconnector': 'NSW1-QLD1',
            'meteredmwflow': 350.5,
            'mwflow': 355.0,
            'mwloss': 4.5,
            'marginalvalue': 12.30
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'interconnector': 'VIC1-SA1',
            'meteredmwflow': -150.0,
            'mwflow': -148.0,
            'mwloss': 2.0,
            'marginalvalue': 8.50
        },
    ])
    await test_db.insert_interconnector_data(interconnector_df)

    # Insert sample generator info
    await test_db.update_generator_info([
        {
            'duid': 'BAYSW1',
            'station_name': 'Bayswater',
            'region': 'NSW',
            'fuel_source': 'Coal',
            'technology_type': 'Steam',
            'capacity_mw': 660
        },
        {
            'duid': 'AGLHAL',
            'station_name': 'Hallett',
            'region': 'SA',
            'fuel_source': 'Wind',
            'technology_type': 'Wind',
            'capacity_mw': 95
        },
        {
            'duid': 'ARWF1',
            'station_name': 'Ararat Wind Farm',
            'region': 'VIC',
            'fuel_source': 'Wind',
            'technology_type': 'Wind',
            'capacity_mw': 240
        },
    ])

    return test_db


# ============================================================================
# Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_dispatch_csv():
    """Sample NEM dispatch CSV content"""
    return SAMPLE_DISPATCH_CSV


@pytest.fixture
def sample_dispatch_zip():
    """Sample NEM dispatch ZIP file"""
    return create_sample_dispatch_zip()


@pytest.fixture
def sample_empty_zip():
    """ZIP file with no CSV"""
    return create_empty_zip()


@pytest.fixture
def sample_directory_html():
    """Sample NEMWEB directory HTML"""
    return SAMPLE_DIRECTORY_HTML


@pytest.fixture
def sample_dispatch_price_csv():
    """Sample dispatch price CSV"""
    return SAMPLE_DISPATCH_PRICE_CSV


@pytest.fixture
def sample_trading_price_csv():
    """Sample trading price CSV"""
    return SAMPLE_TRADING_PRICE_CSV


@pytest.fixture
def sample_public_price_csv():
    """Sample public price CSV"""
    return SAMPLE_PUBLIC_PRICE_CSV


@pytest.fixture
def sample_interconnector_csv():
    """Sample interconnector CSV"""
    return SAMPLE_INTERCONNECTOR_CSV


# ============================================================================
# DataFrame Fixtures
# ============================================================================

@pytest.fixture
def sample_dispatch_df():
    """Sample dispatch DataFrame for database insertion"""
    return pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'duid': 'TEST1',
            'scadavalue': 100.0,
            'uigf': 0.0,
            'totalcleared': 100.0,
            'ramprate': 0.0,
            'availability': 110.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        },
    ])


@pytest.fixture
def sample_price_df():
    """Sample price DataFrame for database insertion"""
    return pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        },
    ])


@pytest.fixture
def sample_interconnector_df():
    """Sample interconnector DataFrame for database insertion"""
    return pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'interconnector': 'NSW1-QLD1',
            'meteredmwflow': 350.5,
            'mwflow': 355.0,
            'mwloss': 4.5,
            'marginalvalue': 12.30
        },
    ])
