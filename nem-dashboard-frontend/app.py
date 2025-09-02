"""
NEM Market Dashboard - Live Prices, Interconnector Flows & Generator Analysis
"""

import vizro
from vizro import Vizro
import vizro.models as vm
import vizro.plotly.express as px
from vizro.actions import export_data
from vizro.models.types import capture
import pandas as pd
import plotly.graph_objects as go
import plotly.express as pe
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import logging
import atexit

from data_service import get_data_service

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize data service
data_service = get_data_service()


# NEM regions and their colors
REGION_COLORS = {
    'NSW': '#1f77b4',
    'VIC': '#ff7f0e', 
    'QLD': '#2ca02c',
    'SA': '#d62728',
    'TAS': '#9467bd'
}

# Dynamic data loading functions with caching
def get_live_prices():
    """Get current dispatch and trading prices"""
    try:
        # Get dispatch prices for demand data (5-minute)
        dispatch_df = data_service.get_latest_prices('DISPATCH')
        
        # Get trading prices for actual price data (30-minute) 
        trading_df = data_service.get_latest_prices('TRADING')
        
        # Combine trading prices with dispatch demand data
        if not trading_df.empty:
            # Use trading prices as primary data
            combined_df = trading_df.copy()
            combined_df['price_type'] = 'Trading (30-min)'
            
            # Add demand data from dispatch if available
            if not dispatch_df.empty:
                demand_map = dispatch_df.set_index('region')['totaldemand'].to_dict()
                combined_df['totaldemand'] = combined_df['region'].map(demand_map).fillna(0.0)
            
        elif not dispatch_df.empty:
            # Fallback to dispatch data if no trading prices
            combined_df = dispatch_df.copy()
            combined_df['price_type'] = 'Dispatch (5-min)'
        else:
            # Return sample data if no real data
            combined_df = pd.DataFrame({
                'region': ['NSW', 'VIC', 'QLD', 'SA', 'TAS'],
                'price': [65.50, 72.30, 58.90, 95.20, 45.70],
                'price_type': ['Sample'] * 5,
                'totaldemand': [8500, 7200, 6800, 2100, 1300]
            })
        
        # Add last updated timestamp from actual data
        if not combined_df.empty and 'settlementdate' in combined_df.columns:
            # Use the actual data timestamp, not current time
            latest_time = pd.to_datetime(combined_df['settlementdate']).max()
            combined_df['last_updated'] = latest_time.strftime('%H:%M:%S')
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Error getting live prices: {e}")
        return pd.DataFrame({
            'region': ['NSW', 'VIC', 'QLD', 'SA', 'TAS'],
            'price': [65.50, 72.30, 58.90, 95.20, 45.70],
            'price_type': ['Sample'] * 5,
            'totaldemand': [8500, 7200, 6800, 2100, 1300],
            'last_updated': ['Sample'] * 5
        })

@capture('graph')
def create_region_cards(data_frame):
    """Create individual cards for each region showing price and demand"""
    df = data_frame
    if df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Get interconnector flows
    interconnector_df = get_interconnector_flows()
    
    # Create a card-like layout using annotations
    last_updated = df['last_updated'].iloc[0] if not df.empty else datetime.now().strftime('%H:%M:%S')
    
    # Add last updated timestamp in top right
    fig.add_annotation(
        text=f"Last Updated: {last_updated}",
        xref="paper", yref="paper",
        x=0.98, y=0.98,
        showarrow=False,
        font=dict(size=12, color="gray"),
        xanchor="right",
        yanchor="top"
    )
    
    # Position regions in a grid layout
    # QLD top right, NSW bot right, VIC middle, SA top left, TAS bot left
    positions = {
        'SA': (0.2, 0.8),   # top left
        'QLD': (0.8, 0.8),  # top right
        'VIC': (0.5, 0.5),  # middle
        'TAS': (0.2, 0.2),  # bot left
        'NSW': (0.8, 0.2)   # bot right
    }
    
    # Define interconnector connections and their midpoints
    interconnector_connections = {
        'NSW1-QLD1': ('NSW', 'QLD'),
        'VIC1-NSW1': ('VIC', 'NSW'), 
        'VIC1-SA1': ('VIC', 'SA'),
        'T-V-MNSP1': ('TAS', 'VIC')
        # Note: VIC1-TAS1 is the same physical connection as T-V-MNSP1, just different direction
    }
    
    for _, row in df.iterrows():
        region = row['region']
        price = row['price']
        demand = row['totaldemand']
        color = REGION_COLORS.get(region, '#333333')
        
        x, y = positions.get(region, (0.5, 0.5))
        
        # Add region name
        fig.add_annotation(
            text=f"<b>{region}</b>",
            x=x, y=y,
            showarrow=False,
            font=dict(size=20, color=color),
            xanchor="center",
            yanchor="middle"
        )
        
        # Add price
        fig.add_annotation(
            text=f"${price:.2f}/MWh",
            x=x, y=y-0.08,
            showarrow=False,
            font=dict(size=16, color="black"),
            xanchor="center",
            yanchor="middle"
        )
        
        # Add demand
        fig.add_annotation(
            text=f"{demand:,.0f} MW",
            x=x, y=y-0.15,
            showarrow=False,
            font=dict(size=14, color="gray"),
            xanchor="center",
            yanchor="middle"
        )
        
        # Add colored background rectangle with border
        fig.add_shape(
            type="rect",
            x0=x-0.12, y0=y-0.18,
            x1=x+0.12, y1=y+0.05,
            fillcolor=color,
            opacity=0.1,
            line=dict(color=color, width=3)
        )
        
        # Add additional border for card effect
        fig.add_shape(
            type="rect",
            x0=x-0.125, y0=y-0.185,
            x1=x+0.125, y1=y+0.055,
            fillcolor="rgba(0,0,0,0)",
            line=dict(color="darkgray", width=1)
        )
    
    # Add interconnector flow arrows between regions (positioned outside cards)
    if not interconnector_df.empty:
        for _, flow_row in interconnector_df.iterrows():
            interconnector = flow_row['interconnector']
            flow = flow_row['mwflow']
            
            # Skip if this interconnector is not in our connections mapping
            if interconnector not in interconnector_connections:
                continue
                
            region1, region2 = interconnector_connections[interconnector]
            
            # Get positions of connected regions
            if region1 not in positions or region2 not in positions:
                continue
                
            x1, y1 = positions[region1]
            x2, y2 = positions[region2]
            
            # Calculate direction vector and normalize
            dx = x2 - x1
            dy = y2 - y1
            length = (dx**2 + dy**2)**0.5
            
            if length > 0:
                # Normalize direction vector
                dx_norm = dx / length
                dy_norm = dy / length
                
                # Calculate edge points (start from card edge, not center)
                card_radius = 0.13  # Slightly larger than card size
                start_x = x1 + dx_norm * card_radius
                start_y = y1 + dy_norm * card_radius
                end_x = x2 - dx_norm * card_radius
                end_y = y2 - dy_norm * card_radius
                
                # Determine flow direction and color
                if flow > 0:
                    # Positive flow: region1 to region2
                    arrow_start_x, arrow_start_y = start_x, start_y
                    arrow_end_x, arrow_end_y = end_x, end_y
                    flow_color = "green"
                elif flow < 0:
                    # Negative flow: region2 to region1
                    arrow_start_x, arrow_start_y = end_x, end_y
                    arrow_end_x, arrow_end_y = start_x, start_y
                    flow_color = "red"
                    flow = abs(flow)  # Use absolute value for display
                else:
                    flow_color = "gray"
                    arrow_start_x, arrow_start_y = start_x, start_y
                    arrow_end_x, arrow_end_y = end_x, end_y
                
                # Add arrow annotation
                fig.add_annotation(
                    x=arrow_end_x,
                    y=arrow_end_y,
                    ax=arrow_start_x,
                    ay=arrow_start_y,
                    arrowhead=2,
                    arrowsize=1.5,
                    arrowwidth=3,
                    arrowcolor=flow_color,
                    text="",  # No text on the arrow itself
                    showarrow=True
                )
                
                # Add flow value annotation at midpoint
                mid_x = (arrow_start_x + arrow_end_x) / 2
                mid_y = (arrow_start_y + arrow_end_y) / 2
                
                # Offset text slightly perpendicular to arrow
                perp_offset = 0.03
                perp_x = -dy_norm * perp_offset
                perp_y = dx_norm * perp_offset
                
                fig.add_annotation(
                    text=f"{flow:.0f}MW",
                    x=mid_x + perp_x,
                    y=mid_y + perp_y,
                    showarrow=False,
                    font=dict(size=10, color=flow_color, weight="bold"),
                    xanchor="center",
                    yanchor="middle",
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor=flow_color,
                    borderwidth=1
                )
    
    fig.update_layout(
        xaxis=dict(visible=False, range=[0, 1]),
        yaxis=dict(visible=False, range=[0, 1]),
        showlegend=False,
        height=500,
        margin=dict(l=0, r=0, t=20, b=0)
    )
    
    return fig

def get_price_history():
    """Get previous day price history"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        # Get PUBLIC price history (complete previous day data with 5-minute intervals)
        df = data_service.get_price_history(start_date, end_date, price_type='PUBLIC')
        
        # If no public data, try trading price history (30-minute intervals)
        if df.empty:
            df = data_service.get_price_history(start_date, end_date, price_type='TRADING')
        
        # If no trading data, fallback to dispatch data
        if df.empty:
            df = data_service.get_price_history(start_date, end_date, price_type='DISPATCH')
        
        if df.empty:
            # Return sample data
            times = pd.date_range(start_date, end_date, freq='30min')[:48]  # 48 half-hours
            sample_data = []
            for region in ['NSW', 'VIC', 'QLD', 'SA', 'TAS']:
                for time in times:
                    sample_data.append({
                        'region': region,
                        'settlementdate': time,
                        'price': 50 + (hash(f"{region}{time}") % 100),
                        'price_type': 'Sample'
                    })
            df = pd.DataFrame(sample_data)
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting price history: {e}")
        return pd.DataFrame()

def get_interconnector_flows():
    """Get current interconnector flows"""
    try:
        df = data_service.get_latest_interconnector_flows()
        
        if df.empty:
            # Return sample interconnector data
            df = pd.DataFrame({
                'interconnector': ['NSW1-QLD1', 'VIC1-NSW1', 'VIC1-SA1', 'T-V-MNSP1', 'VIC1-TAS1'],
                'mwflow': [450, -650, 200, 350, -180],
                'marginalvalue': [65.50, 72.30, 95.20, 45.70, 72.30],
                'direction': ['NSW→QLD', 'VIC→NSW', 'VIC→SA', 'TAS→VIC', 'VIC→TAS']
            })
        else:
            # Add direction labels for better visualization
            direction_map = {
                'NSW1-QLD1': 'NSW→QLD',
                'VIC1-NSW1': 'VIC→NSW',
                'VIC1-SA1': 'VIC→SA',
                'T-V-MNSP1': 'TAS→VIC',
                'VIC1-TAS1': 'VIC→TAS'
            }
            df['direction'] = df['interconnector'].map(direction_map).fillna(df['interconnector'])
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting interconnector flows: {e}")
        return pd.DataFrame()

def get_generators_by_filter(region=None, fuel_source=None):
    """Get generators with region/fuel filtering"""
    try:
        df = data_service.get_generators_by_region_fuel(region, fuel_source)
        
        if df.empty:
            # Return sample generator data
            sample_data = []
            regions = [region] if region else ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
            fuels = [fuel_source] if fuel_source else ['Coal', 'Gas', 'Wind', 'Solar', 'Hydro']
            
            for r in regions:
                for f in fuels:
                    for i in range(2):  # 2 generators per region/fuel combo
                        sample_data.append({
                            'duid': f'{r}{f[:3].upper()}{i+1}',
                            'scadavalue': abs(hash(f"{r}{f}{i}") % 500),
                            'region': r,
                            'fuel_source': f,
                            'station_name': f'{r} {f} Station {i+1}',
                            'capacity_mw': 200 + (hash(f"{r}{f}{i}") % 300)
                        })
            
            df = pd.DataFrame(sample_data)
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting generators: {e}")
        return pd.DataFrame()


# Dashboard Pages

# 1. Live Prices & Interconnectors Page
live_prices_page = vm.Page(
    title="Live Prices & Flows",
    components=[
        vm.Graph(
            title="Live NEM Regional Prices & Demand",
            figure=create_region_cards(get_live_prices())
        ),
        vm.Graph(
            figure=px.bar(
                get_interconnector_flows(),
                x="direction",
                y="mwflow",
                color="mwflow",
                title="Current Interconnector Flows (MW)",
                labels={
                    "mwflow": "Flow (MW)",
                    "direction": "Interconnector"
                },
                color_continuous_scale="RdYlBu_r",
                hover_data=["marginalvalue"],
                height=400
            ),
        ),
    ],
)

# 2. Price History Page  
price_history_page = vm.Page(
    title="Price History",
    components=[
        vm.Graph(
            figure=px.line(
                get_price_history(),
                x="settlementdate",
                y="price",
                color="region",
                title="Price History - Previous Day (5-min intervals)",
                labels={
                    "settlementdate": "Time",
                    "price": "Price ($/MWh)"
                },
                color_discrete_map=REGION_COLORS,
                height=500
            ),
        ),
        vm.Graph(
            figure=px.box(
                get_price_history(),
                x="region",
                y="price",
                color="region",
                title="Price Distribution by Region",
                labels={
                    "price": "Price ($/MWh)",
                    "region": "NEM Region"
                },
                color_discrete_map=REGION_COLORS,
                height=400
            ),
        ),
    ],
)

# 3. Generator Analysis Page - Split into fewer, larger graphs
generator_analysis_page = vm.Page(
    title="Generator Analysis",
    components=[
        vm.Graph(
            figure=px.scatter(
                get_generators_by_filter().assign(abs_output=lambda df: df['scadavalue'].abs() + 1),
                x="capacity_mw",
                y="scadavalue",
                color="fuel_source",
                size="abs_output",
                hover_data=["duid", "station_name", "region"],
                title="Generator Output vs Capacity - All Generators",
                labels={
                    "capacity_mw": "Capacity (MW)",
                    "scadavalue": "Current Output (MW)"
                },
                height=500
            ),
        ),
        vm.Graph(
            figure=px.bar(
                get_generators_by_filter().groupby(['region', 'fuel_source'])['scadavalue'].sum().reset_index(),
                x="region",
                y="scadavalue",
                color="fuel_source",
                title="Total Generation Output by Region and Fuel Type",
                labels={
                    "scadavalue": "Output (MW)",
                    "region": "NEM Region"
                },
                height=450
            ),
        ),
    ],
)

# 4. All Generators Page (single comprehensive view - sorted by current output)
all_generators_page = vm.Page(
    title="All Generators", 
    components=[
        vm.Graph(
            figure=px.bar(
                get_generators_by_filter().sort_values('scadavalue', ascending=True).tail(100),
                x="scadavalue",
                y="station_name",
                orientation="h", 
                color="fuel_source",
                title="Top 100 Generators by Current Output (MW) - Click legend to filter fuel types",
                labels={
                    "scadavalue": "Current Output (MW)",
                    "station_name": "Generator"
                },
                hover_data=["duid", "capacity_mw", "region"],
                height=1200
            ),
        ),
    ],
)

# Create dashboard with useful market data
dashboard = vm.Dashboard(
    title="NEM Market Dashboard",
    pages=[live_prices_page, price_history_page, generator_analysis_page, all_generators_page],
)

# Initialize Vizro app
app = Vizro().build(dashboard)

if __name__ == "__main__":
    # Get configuration from environment
    port = int(os.getenv('DASHBOARD_PORT', 8050))
    host = os.getenv('DASHBOARD_HOST', '0.0.0.0')
    debug = os.getenv('DEBUG', 'True').lower() == 'true'
    
    logger.info(f"Starting NEM Market Dashboard on {host}:{port}")
    
    # Start the Vizro app
    app.run(
        host=host,
        port=port,
        debug=debug
    )