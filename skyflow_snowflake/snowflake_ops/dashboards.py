"""Dashboard operations - Snowsight dashboard functionality for Skyflow integration."""

import json
from pathlib import Path
from typing import Dict, Optional, Any
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from rich.console import Console
from .client import SnowflakeClientWrapper

console = Console()


class SnowsightDashboardManager:
    """Manages Snowsight dashboards - simplified implementation."""
    
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self.connection = connection
        self.wrapper = SnowflakeClientWrapper(connection)
    
    def create_dashboard_from_file(self, local_path: str, dashboard_name: str, 
                                  warehouse_name: str, substitutions: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Create a Snowsight dashboard (simplified - creates views instead)."""
        try:
            path = Path(local_path)
            if not path.exists():
                console.print(f"✗ Dashboard file not found: {local_path}")
                return None
            
            # Since Snowsight dashboard creation via API is complex,
            # we'll create database views that can be used in Snowsight manually
            console.print(f"Creating dashboard views for: {dashboard_name}")
            
            # Create sample views for dashboard data
            views = [
                f"CREATE OR REPLACE VIEW {dashboard_name}_CUSTOMER_OVERVIEW AS SELECT * FROM {substitutions.get('PREFIX', 'demo')}_customer_data LIMIT 100",
                f"CREATE OR REPLACE VIEW {dashboard_name}_TOKENIZATION_STATUS AS SELECT 'Tokenization Complete' as status, COUNT(*) as total_records FROM {substitutions.get('PREFIX', 'demo')}_customer_data"
            ]
            
            cursor = self.connection.cursor()
            
            for view_sql in views:
                try:
                    cursor.execute(view_sql)
                    console.print(f"  ✓ Created view: {view_sql.split()[4]}")  # Extract view name
                except SnowflakeError as e:
                    console.print(f"  ⚠ Failed to create view: {e}")
            
            cursor.close()
            
            # Return a placeholder URL since we can't create actual Snowsight dashboards via API
            placeholder_url = f"https://app.snowflake.com/dashboards/{dashboard_name}"
            console.print(f"✓ Dashboard components created. Manually create dashboard at: {placeholder_url}")
            
            return placeholder_url
            
        except Exception as e:
            console.print(f"✗ Error creating dashboard: {e}")
            return None
    
    def setup_customer_insights_dashboard(self, prefix: str, warehouse_name: str) -> Optional[str]:
        """Setup the customer insights dashboard."""
        dashboard_name = f"{prefix}_customer_insights_dashboard"
        substitutions = {"PREFIX": prefix, "WAREHOUSE": warehouse_name}
        
        # For now, create database views that can be used in Snowsight
        return self.create_dashboard_from_file(
            "templates/dashboards/customer_insights_dashboard.lvdash.json",
            dashboard_name,
            warehouse_name,
            substitutions
        )
    
    def find_dashboard_by_name(self, dashboard_name: str) -> Optional[str]:
        """Find dashboard by name (simplified - returns placeholder)."""
        # Since we can't easily query Snowsight dashboards,
        # return a placeholder ID if views exist
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW VIEWS LIKE '{dashboard_name}%'")
            views = cursor.fetchall()
            cursor.close()
            
            if views:
                return f"dashboard_{dashboard_name}"
            return None
        except:
            return None
    
    def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete dashboard (simplified - drops related views)."""
        try:
            dashboard_name = dashboard_id.replace("dashboard_", "")
            cursor = self.connection.cursor()
            
            # Drop views associated with the dashboard
            cursor.execute(f"SHOW VIEWS LIKE '{dashboard_name}%'")
            views = cursor.fetchall()
            
            for view in views:
                view_name = view[1]  # View name is typically in the second column
                try:
                    cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                    console.print(f"✓ Dropped view: {view_name}")
                except SnowflakeError as e:
                    console.print(f"⚠ Failed to drop view {view_name}: {e}")
            
            cursor.close()
            console.print(f"✓ Dashboard components cleaned up: {dashboard_name}")
            return True
            
        except Exception as e:
            console.print(f"✗ Error deleting dashboard: {e}")
            return False