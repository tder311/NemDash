"""
Simple data service for NEM Dashboard frontend
Connects directly to backend API endpoints
"""

import pandas as pd
import requests
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataService:
    """Simple data service that connects to backend API"""
    
    def __init__(self, backend_url: str = "http://localhost:8000"):
        self.backend_url = backend_url.rstrip('/')
        
    def get_latest_prices(self, price_type: str = 'TRADING'):
        """Get latest regional prices"""
        try:
            params = {'price_type': price_type}
            response = requests.get(f"{self.backend_url}/api/prices/latest", params=params)
            if response.status_code == 200:
                data = response.json()
                return pd.DataFrame(data.get('data', []))
            else:
                logger.warning(f"Failed to fetch prices: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching prices: {e}")
            return pd.DataFrame()
    
    def get_price_history(self, start_date: datetime, end_date: datetime, price_type: str = 'TRADING'):
        """Get historical price data"""
        try:
            params = {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'price_type': price_type
            }
            response = requests.get(f"{self.backend_url}/api/prices/history", params=params)
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data.get('data', []))
                if not df.empty and 'settlementdate' in df.columns:
                    df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                return df
            else:
                logger.warning(f"Failed to fetch price history: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching price history: {e}")
            return pd.DataFrame()
    
    def get_latest_interconnector_flows(self):
        """Get current interconnector flows"""
        try:
            response = requests.get(f"{self.backend_url}/api/interconnectors/latest")
            if response.status_code == 200:
                data = response.json()
                return pd.DataFrame(data.get('data', []))
            else:
                logger.warning(f"Failed to fetch interconnector flows: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching interconnector flows: {e}")
            return pd.DataFrame()
    
    def get_generators_by_region_fuel(self, region: str = None, fuel_source: str = None):
        """Get generators with optional filtering"""
        try:
            params = {}
            if region:
                params['region'] = region
            if fuel_source:
                params['fuel_source'] = fuel_source
                
            response = requests.get(f"{self.backend_url}/api/generators/filter", params=params)
            if response.status_code == 200:
                data = response.json()
                return pd.DataFrame(data.get('data', []))
            else:
                logger.warning(f"Failed to fetch generators: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching generators: {e}")
            return pd.DataFrame()

def get_data_service():
    """Get data service instance"""
    return DataService()