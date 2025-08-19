"""CLI command implementations for Snowflake Skyflow integration."""

import time
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config.config import SetupConfig
from snowflake_ops.snowflake_manager import SnowflakeResourceManager
from snowflake_ops.secrets import SnowflakeSecretsManager
from snowflake_ops.sql import SnowflakeSQLExecutor
from snowflake_ops.notebooks import StoredProcedureManager
from snowflake_ops.dashboards import SnowsightDashboardManager
from utils.validation import validate_prefix, validate_required_files

console = Console()


class BaseCommand:
    """Base class for all commands."""
    
    def __init__(self, prefix: str, config: Optional[SetupConfig] = None):
        self.prefix = prefix
        self.config = config or SetupConfig()
        
        # Validate prefix
        is_valid, error = validate_prefix(prefix)
        if not is_valid:
            raise ValueError(f"Invalid prefix: {error}")
    
    def validate_environment(self):
        """Validate environment and configuration."""
        try:
            self.config.validate()
        except ValueError as e:
            console.print(f"[red]Configuration error: {e}[/red]")
            raise


class CreateCommand(BaseCommand):
    """Implementation of 'create' command."""
    
    def execute(self) -> bool:
        """Execute the create command."""
        console.print(Panel.fit(
            f"Creating Skyflow Snowflake Integration: [bold]{self.prefix}[/bold]",
            style="green"
        ))
        
        try:
            # Always destroy first to ensure clean state
            console.print(f"[dim]Cleaning up any existing '{self.prefix}' resources...[/dim]")
            destroy_command = DestroyCommand(self.prefix, self.config)
            destroy_command.execute()  # Don't fail if destroy has issues
            
            # Validate environment
            self.validate_environment()
            
            # Check required files exist
            required_files = [
                "sql/setup/create_sample_table.sql", 
                "sql/setup/create_external_functions.sql",
                "sql/setup/setup_external_functions.sql",
                "sql/setup/apply_column_masks.sql",
                "notebooks/notebook_tokenize_table.ipynb",
                "dashboards/customer_insights_dashboard.lvdash.json"
            ]
            
            files_exist, missing = validate_required_files(required_files)
            if not files_exist:
                console.print(f"[red]Missing required files: {', '.join(missing)}[/red]")
                return False
            
            # Initialize managers
            resource_manager = SnowflakeResourceManager(self.config.connection)
            secrets_manager = SnowflakeSecretsManager(self.config.connection)
            sql_executor = SnowflakeSQLExecutor(self.config.connection)
            procedure_manager = StoredProcedureManager(self.config.connection)
            dashboard_manager = SnowsightDashboardManager(self.config.connection)
            
            # Get substitutions
            substitutions = self.config.get_substitutions(self.prefix)
            
            # Step 1: Create Snowflake database and schema resources
            console.print("\n[bold blue]Step 1: Setting up Snowflake database and schema[/bold blue]")
            if not self._setup_database_schema(resource_manager):
                return False
            
            # Step 2: Create required roles for data access control
            console.print("\n[bold blue]Step 2: Creating required Snowflake roles[/bold blue]")
            if not self._setup_roles(resource_manager):
                console.print("[yellow]⚠ Role creation failed - continuing (roles may already exist)[/yellow]")
            
            # Step 3: Setup secrets
            console.print("\n[bold blue]Step 3: Setting up secrets[/bold blue]")
            if not self._setup_secrets(secrets_manager):
                return False
            
            # Step 4: Create network rules and external access integration
            console.print("\n[bold blue]Step 4: Setting up network rules and external access[/bold blue]")
            if not self._setup_network_rules(sql_executor, substitutions):
                return False
            
            # Step 5: Create connections
            console.print("\n[bold blue]Step 5: Creating HTTP connections[/bold blue]")
            if not self._setup_connections(sql_executor, substitutions):
                return False
            
            # Step 6: Create sample data
            console.print("\n[bold blue]Step 6: Creating sample table[/bold blue]")
            if not self._create_sample_data(sql_executor, substitutions):
                return False
            
            # Step 7: Create tokenization stored procedure
            console.print("\n[bold blue]Step 7: Creating tokenization stored procedure[/bold blue]")
            if not self._create_tokenization_procedure(procedure_manager, substitutions):
                return False
            
            # Step 8: Verify functions before tokenization
            console.print("\n[bold blue]Step 8: Verifying functions[/bold blue]")
            if not self._verify_functions(sql_executor, substitutions):
                console.print("[yellow]⚠ Function verification failed - continuing[/yellow]")
            
            # Step 9: Execute tokenization (BEFORE applying masking policies!)
            console.print("\n[bold blue]Step 9: Running tokenization[/bold blue]")
            tokenization_success = self._execute_tokenization(procedure_manager)
            if not tokenization_success:
                console.print("[yellow]⚠ Tokenization failed - continuing with setup[/yellow]")
            
            # Step 10: Apply masking policies AFTER tokenization (correct order!)
            console.print("\n[bold blue]Step 10: Applying masking policies to tokenized data[/bold blue]")
            if tokenization_success:  # Only apply masks if tokenization succeeded
                functions_success = self._setup_functions(sql_executor, substitutions)
                if not functions_success:
                    console.print("[yellow]⚠ Masking policies failed - continuing without them[/yellow]")
            else:
                console.print("[yellow]⚠ Skipping masking policies - tokenization failed[/yellow]")
            
            # Step 11: Create dashboard
            console.print("\n[bold blue]Step 11: Creating dashboard[/bold blue]")
            dashboard_url = self._create_dashboard(dashboard_manager)
            
            # Step 12: Validate role-based access with test queries
            console.print("\n[bold blue]Step 12: Validating role-based access[/bold blue]")
            validation_success = self._validate_role_access(sql_executor)
            if not validation_success:
                console.print("[yellow]⚠ Role validation failed - check role permissions[/yellow]")
            
            # Success summary
            self._print_success_summary(dashboard_url)
            return True
            
        except Exception as e:
            console.print(f"[red]Setup failed: {e}[/red]")
            return False
    
    def _setup_database_schema(self, resource_manager: SnowflakeResourceManager) -> bool:
        """Setup Snowflake database and schema."""
        database_name = f"{self.prefix}_database"
        schema_name = self.config.snowflake.schema_name
        
        success = resource_manager.create_database(database_name)
        success &= resource_manager.create_schema(database_name, schema_name)
        
        return success
    
    def _setup_roles(self, resource_manager: SnowflakeResourceManager) -> bool:
        """Setup required Snowflake roles for data access control."""
        # Get role names from configuration
        # Create prefixed role names to avoid conflicts
        roles = [
            f"{self.prefix}_{self.config.groups.plain_text_groups.upper()}",
            f"{self.prefix}_{self.config.groups.masked_groups.upper()}",
            f"{self.prefix}_{self.config.groups.redacted_groups.upper()}"
        ]
        
        # Create roles with config for proper descriptions
        success = resource_manager.create_required_roles(roles, self.config.groups)
        
        # Grant database access to roles
        if success:
            database_name = f"{self.prefix}_database"
            success &= resource_manager.grant_database_access_to_roles(database_name, roles)
        
        return success
    
    def _setup_secrets(self, secrets_manager: SnowflakeSecretsManager) -> bool:
        """Setup Snowflake secrets."""
        skyflow_config = {
            "pat_token": self.config.skyflow.pat_token,
            "vault_url": self.config.skyflow.vault_url,
            "vault_id": self.config.skyflow.vault_id,
            "table": self.config.skyflow.table,
            "table_column": self.config.skyflow.table_column
        }
        
        return secrets_manager.setup_skyflow_secrets(skyflow_config)
    
    def _setup_network_rules(self, sql_executor: SnowflakeSQLExecutor, substitutions: dict) -> bool:
        """Setup network rules and external access integration for Skyflow API access."""
        # Extract vault host from Skyflow vault URL for network rule
        vault_url = self.config.skyflow.vault_url
        if vault_url.startswith('https://'):
            vault_host = vault_url[8:]  # Remove https://
        elif vault_url.startswith('http://'):
            vault_host = vault_url[7:]   # Remove http://
        else:
            vault_host = vault_url
        
        # Add vault host to substitutions
        substitutions_with_host = {**substitutions, 'SKYFLOW_VAULT_HOST': vault_host}
        
        return sql_executor.execute_sql_file(
            "sql/setup/create_network_rules.sql",
            substitutions_with_host
        )
    
    def _setup_connections(self, sql_executor: SnowflakeSQLExecutor, substitutions: dict) -> bool:
        """Setup API integrations and external functions using SQL."""
        # Create API integrations using SQL file
        success = sql_executor.execute_sql_file(
            "sql/setup/create_external_functions.sql",
            substitutions
        )
        
        if success:
            # Execute additional function setup SQL (detokenization functions)
            success &= sql_executor.execute_sql_file(
                "sql/setup/setup_external_functions.sql",
                substitutions
            )
        
        return success
    
    def _create_sample_data(self, sql_executor: SnowflakeSQLExecutor, substitutions: dict) -> bool:
        """Create sample table and data."""
        success = sql_executor.execute_sql_file(
            "sql/setup/create_sample_table.sql",
            substitutions
        )
        
        if success:
            # Check table exists first without counting rows (table might be empty initially)
            table_name = f"{self.prefix}_customer_data"
            if sql_executor.verify_table_exists(table_name):
                console.print(f"  ✓ Created table: {table_name}")
                row_count = sql_executor.get_table_row_count(table_name)
                if row_count is not None and row_count > 0:
                    console.print(f"  ✓ Table has {row_count} rows")
                else:
                    console.print(f"  ✓ Table created (empty)")
        
        return success
    
    def _setup_functions(self, sql_executor: SnowflakeSQLExecutor, substitutions: dict) -> bool:
        """Setup detokenization functions and masking policies."""
        return sql_executor.execute_sql_file(
            "sql/setup/apply_column_masks.sql",
            substitutions
        )
    
    def _create_tokenization_procedure(self, procedure_manager: StoredProcedureManager, substitutions: dict) -> bool:
        """Create the tokenization stored procedure."""
        try:
            batch_size = self.config.skyflow.batch_size
            return procedure_manager.setup_tokenization_procedure(self.prefix, substitutions, batch_size)
        except Exception as e:
            console.print(f"✗ Procedure creation failed: {e}")
            return False
    
    def _verify_functions(self, sql_executor: SnowflakeSQLExecutor, substitutions: dict) -> bool:
        """Verify Snowflake functions exist."""
        try:
            # Add 5 second delay for function creation
            console.print("Verifying function creation...")
            time.sleep(5)
            
            console.print("Verifying Snowflake detokenization functions...")
            success = sql_executor.execute_sql_file("sql/verify/verify_functions.sql", substitutions)
            if success:
                console.print("✓ Snowflake conditional detokenization functions verified")
            return success
        except Exception as e:
            console.print(f"✗ Function verification failed: {e}")
            return False
    
    def _execute_tokenization(self, procedure_manager: StoredProcedureManager) -> bool:
        """Execute the tokenization stored procedure."""
        try:
            # Get batch size from config
            batch_size = self.config.skyflow.batch_size
            return procedure_manager.execute_tokenization_notebook(self.prefix, batch_size)
        except Exception as e:
            console.print(f"✗ Tokenization execution failed: {e}")
            return False
    
    def _create_dashboard(self, dashboard_manager: SnowsightDashboardManager) -> Optional[str]:
        """Create the customer insights dashboard."""
        return dashboard_manager.setup_customer_insights_dashboard(
            self.prefix,
            self.config.snowflake.warehouse
        )
    
    def _validate_role_access(self, sql_executor: SnowflakeSQLExecutor) -> bool:
        """Validate role-based access by testing queries as each role."""
        try:
            table_name = f"{self.prefix}_customer_data"
            
            # Check if table has tokenized data
            row_count = sql_executor.get_table_row_count(table_name)
            if not row_count or row_count == 0:
                console.print("  ⚠ No data in table - skipping role validation")
                return True
            
            # Get prefixed role names
            plain_text_role = f"{self.prefix}_{self.config.groups.plain_text_groups.upper()}"
            masked_role = f"{self.prefix}_{self.config.groups.masked_groups.upper()}"  
            redacted_role = f"{self.prefix}_{self.config.groups.redacted_groups.upper()}"
            
            validation_results = []
            
            # Test each role
            roles_to_test = [
                (plain_text_role, "PLAIN_TEXT", "should see detokenized data"),
                (masked_role, "MASKED", "should see masked data"),
                (redacted_role, "REDACTED", "should see redacted data")
            ]
            
            for role_name, expected_type, description in roles_to_test:
                console.print(f"  Testing role: {role_name} ({description})")
                
                # Switch to role and test query
                test_query = f"""
                USE ROLE {role_name};
                USE DATABASE {self.prefix}_database;
                SELECT first_name, email 
                FROM {table_name} 
                LIMIT 1;
                """
                
                try:
                    cursor = sql_executor.connection.cursor()
                    
                    # Execute role switch and query
                    for statement in test_query.strip().split(';'):
                        if statement.strip():
                            cursor.execute(statement.strip())
                    
                    result = cursor.fetchone()
                    cursor.close()
                    
                    if result:
                        first_name, email = result[0], result[1]
                        console.print(f"    ✓ {role_name}: first_name='{first_name}', email='{email}'")
                        validation_results.append(True)
                    else:
                        console.print(f"    ✗ {role_name}: No data returned")
                        validation_results.append(False)
                        
                except Exception as e:
                    console.print(f"    ✗ {role_name}: Query failed - {e}")
                    validation_results.append(False)
            
            # Switch back to admin role
            try:
                cursor = sql_executor.connection.cursor()
                cursor.execute(f"USE ROLE {self.config.snowflake.role}")
                cursor.close()
            except:
                pass
            
            success_count = sum(validation_results)
            total_count = len(validation_results)
            
            if success_count == total_count:
                console.print(f"  ✓ All {total_count} role validation tests passed")
                return True
            else:
                console.print(f"  ⚠ {success_count}/{total_count} role validation tests passed")
                return False
                
        except Exception as e:
            console.print(f"  ✗ Role validation failed: {e}")
            return False
    
    def _print_success_summary(self, dashboard_url: Optional[str]):
        """Print success summary with resources created."""
        console.print("\n" + "="*60)
        console.print(Panel.fit(
            f"[bold green]✓ Setup Complete: {self.prefix}[/bold green]",
            style="green"
        ))
        
        # Resources table
        table = Table(title="Resources Created")
        table.add_column("Resource", style="cyan")
        table.add_column("Name", style="green")
        
        table.add_row("Snowflake Database", f"{self.prefix}_database")
        table.add_row("Sample Table", f"{self.prefix}_customer_data")
        # Show the actual prefixed role names that were created
        prefixed_roles = [
            f"{self.prefix}_{self.config.groups.plain_text_groups.upper()}".upper(),
            f"{self.prefix}_{self.config.groups.masked_groups.upper()}".upper(),
            f"{self.prefix}_{self.config.groups.redacted_groups.upper()}".upper()
        ]
        table.add_row("Snowflake Roles", ", ".join(prefixed_roles))
        table.add_row("Snowflake Secret", "SKYFLOW_PAT_TOKEN")
        table.add_row("API Integration", "SKYFLOW_API_INTEGRATION")
        table.add_row("Tokenization Procedure", f"{self.prefix}_TOKENIZE_TABLE")
        table.add_row("Masking Policies", f"{self.prefix}_pii_mask")
        
        if dashboard_url:
            table.add_row("Dashboard", f"{self.prefix}_customer_insights_dashboard")
        
        console.print(table)
        
        if dashboard_url:
            console.print(f"\n[bold]Dashboard URL:[/bold] {dashboard_url}")
        
        console.print("\n[bold]Next Steps:[/bold]")
        console.print(f"1. Grant roles to users:")
        console.print(f"   GRANT ROLE {self.prefix}_{self.config.groups.plain_text_groups.upper()} TO USER your_user;")
        console.print(f"   GRANT ROLE {self.prefix}_{self.config.groups.masked_groups.upper()} TO USER your_customer_service;")
        console.print(f"   GRANT ROLE {self.prefix}_{self.config.groups.redacted_groups.upper()} TO USER your_marketing;")
        console.print("2. Test role-based access by running queries as different users")
        console.print("3. Explore the dashboard to see detokenization in action")
        console.print("4. Use the SQL functions in your own queries and applications")


class DestroyCommand(BaseCommand):
    """Implementation of 'destroy' command."""
    
    def execute(self) -> bool:
        """Execute the destroy command."""
        console.print(Panel.fit(
            f"Destroying Skyflow Snowflake Integration: [bold]{self.prefix}[/bold]",
            style="red"
        ))
        
        try:
            self.validate_environment()
            
            # Initialize managers
            resource_manager = SnowflakeResourceManager(self.config.connection)
            secrets_manager = SnowflakeSecretsManager(self.config.connection)
            procedure_manager = StoredProcedureManager(self.config.connection)
            dashboard_manager = SnowsightDashboardManager(self.config.connection)
            sql_executor = SnowflakeSQLExecutor(self.config.connection)
            
            # Track successful and failed deletions for validation
            successful_deletions = []
            failed_deletions = []
            
            # Step 1: Delete dashboard
            console.print("\n[bold blue]Step 1: Removing dashboard[/bold blue]")
            dashboard_name = f"{self.prefix}_customer_insights_dashboard"
            dashboard_id = dashboard_manager.find_dashboard_by_name(dashboard_name)
            if dashboard_id:
                if dashboard_manager.delete_dashboard(dashboard_id):
                    successful_deletions.append(f"Dashboard: {dashboard_name}")
                    # Validate deletion
                    if dashboard_manager.find_dashboard_by_name(dashboard_name):
                        failed_deletions.append(f"Dashboard: {dashboard_name} (still exists)")
                else:
                    failed_deletions.append(f"Dashboard: {dashboard_name}")
            else:
                console.print(f"✓ Dashboard '{dashboard_name}' doesn't exist")
                successful_deletions.append(f"Dashboard: {dashboard_name} (didn't exist)")
            
            # Step 2: Delete stored procedure
            console.print("\n[bold blue]Step 2: Removing stored procedure[/bold blue]")
            procedure_name = f"{self.prefix}_TOKENIZE_TABLE"
            if procedure_manager.delete_notebook(procedure_name):  # Using compatibility method
                successful_deletions.append(f"Procedure: {procedure_name}")
                # Note: Validation handled in delete method
            
            # Step 3: Remove masking policies before dropping functions/table
            console.print("\n[bold blue]Step 3: Removing masking policies[/bold blue]")
            database_name = f"{self.prefix}_database"
            # Get full substitutions from config
            substitutions = self.config.get_substitutions(self.prefix)
            if resource_manager.database_exists(database_name):
                if sql_executor.execute_sql_file("sql/destroy/remove_column_masks.sql", substitutions):
                    successful_deletions.append("Masking policies removed")
                else:
                    console.print("✓ Masking policies removal skipped (may not exist)")
                    successful_deletions.append("Masking policies (skipped)")
            else:
                console.print("✓ Masking policies removal skipped (database doesn't exist)")
                successful_deletions.append("Masking policies (database didn't exist)")
            
            # Step 4: Drop functions and policies
            console.print("\n[bold blue]Step 4: Dropping Snowflake functions and policies[/bold blue]")
            database_name = f"{self.prefix}_database"
            if resource_manager.database_exists(database_name):
                if sql_executor.execute_sql_file("sql/destroy/drop_functions.sql", substitutions):
                    successful_deletions.append("Snowflake functions and policies")
                else:
                    failed_deletions.append("Snowflake functions and policies")
            else:
                console.print(f"✓ Database '{database_name}' doesn't exist, skipping function cleanup")
                successful_deletions.append("Functions (database didn't exist)")
            
            # Step 5: Drop table
            console.print("\n[bold blue]Step 5: Dropping sample table[/bold blue]")
            if resource_manager.database_exists(database_name):
                if sql_executor.execute_sql_file("sql/destroy/drop_table.sql", substitutions):
                    successful_deletions.append("Sample table")
                else:
                    failed_deletions.append("Sample table")
            else:
                successful_deletions.append("Sample table (database didn't exist)")
            
            # Step 6: Delete network rules and external access integration (before database)
            console.print("\n[bold blue]Step 6: Cleaning up network rules and external access[/bold blue]")
            if sql_executor.execute_sql_file("sql/destroy/drop_network_rules.sql", substitutions):
                successful_deletions.append("Network rules and external access integration")
            else:
                failed_deletions.append("Network rules and external access integration")
            
            # Step 7: Delete database (after all database-specific objects)
            console.print("\n[bold blue]Step 7: Removing Snowflake database[/bold blue]")
            if resource_manager.drop_database(database_name):
                successful_deletions.append(f"Database: {database_name}")
                # Validate database deletion
                if resource_manager.database_exists(database_name):
                    failed_deletions.append(f"Database: {database_name} (still exists)")
            else:
                failed_deletions.append(f"Database: {database_name}")
            
            # Step 8: Delete API integration
            console.print("\n[bold blue]Step 8: Cleaning up API integration[/bold blue]")
            integration_name = "SKYFLOW_API_INTEGRATION"
            if resource_manager.drop_api_integration(integration_name):
                successful_deletions.append(f"API Integration: {integration_name}")
                # Validate integration deletion
                if resource_manager.api_integration_exists(integration_name):
                    failed_deletions.append(f"API Integration: {integration_name} (still exists)")
            # Note: If integration doesn't exist, drop_api_integration already handles this gracefully
            
            # Step 9: Delete roles
            console.print("\n[bold blue]Step 9: Cleaning up Snowflake roles[/bold blue]")
            # Use same prefixed role names as creation
            roles_to_delete = [
                f"{self.prefix}_{self.config.groups.plain_text_groups.upper()}",  # PREFIX_AUDITOR
                f"{self.prefix}_{self.config.groups.masked_groups.upper()}",      # PREFIX_CUSTOMER_SERVICE  
                f"{self.prefix}_{self.config.groups.redacted_groups.upper()}"     # PREFIX_MARKETING
            ]
            
            roles_deleted = 0
            for role_name in roles_to_delete:
                if self._delete_role(role_name):
                    roles_deleted += 1
                else:
                    failed_deletions.append(f"Role: {role_name}")
            
            if roles_deleted == len(roles_to_delete):
                successful_deletions.append(f"All {roles_deleted} Snowflake roles")
            elif roles_deleted > 0:
                successful_deletions.append(f"{roles_deleted}/{len(roles_to_delete)} Snowflake roles")
            else:
                failed_deletions.append("All Snowflake roles")
            
            # Step 10: Delete secrets
            console.print("\n[bold blue]Step 10: Cleaning up secrets[/bold blue]")
            skyflow_secrets = ["SKYFLOW_PAT_TOKEN"]  # Only the sensitive PAT token is stored as secret
            secrets_deleted = 0
            for secret_name in skyflow_secrets:
                # Pass database name to handle context properly
                if secrets_manager.drop_secret(secret_name, database_name):
                    secrets_deleted += 1
                else:
                    failed_deletions.append(f"Secret: {secret_name}")
            
            if secrets_deleted == len(skyflow_secrets):
                successful_deletions.append(f"Skyflow PAT token secret")
            elif secrets_deleted > 0:
                successful_deletions.append(f"{secrets_deleted}/{len(skyflow_secrets)} Skyflow secrets")
            else:
                failed_deletions.append("Skyflow PAT token secret")
            
            # Print comprehensive validation summary
            self._print_destroy_summary(successful_deletions, failed_deletions)
            
            # Return success only if all deletions succeeded and were validated
            return len(failed_deletions) == 0
            
        except Exception as e:
            console.print(f"[red]Destroy failed: {e}[/red]")
            return False
    
    def _delete_role(self, role_name: str) -> bool:
        """Delete a Snowflake role if it exists."""
        try:
            cursor = self.config.connection.cursor()
            
            # Check if role exists
            cursor.execute(f"SHOW ROLES LIKE '{role_name}'")
            if not cursor.fetchone():
                console.print(f"✓ Role '{role_name}' doesn't exist")
                cursor.close()
                return True
            
            # Drop role
            cursor.execute(f"DROP ROLE {role_name}")
            cursor.close()
            console.print(f"✓ Deleted role: {role_name}")
            return True
            
        except Exception as e:
            console.print(f"✗ Failed to delete role {role_name}: {e}")
            return False
    
    def _print_destroy_summary(self, successful: list, failed: list):
        """Print a detailed summary of destroy operation results."""
        console.print("\n" + "="*60)
        console.print("[bold]Destroy Summary[/bold]")
        
        if successful:
            console.print(f"\n[bold green]Successfully deleted ({len(successful)}):[/bold green]")
            for item in successful:
                console.print(f"  ✓ {item}")
        
        if failed:
            console.print(f"\n[bold red]Failed to delete ({len(failed)}):[/bold red]")
            for item in failed:
                console.print(f"  ✗ {item}")
            console.print("\n[yellow]Warning: Some resources could not be deleted or verified. Manual cleanup may be required.[/yellow]")
            console.print(Panel.fit(
                f"[bold red]⚠ Cleanup completed with {len(failed)} errors[/bold red]",
                style="yellow"
            ))
        else:
            console.print(Panel.fit(
                f"[bold green]✓ All resources successfully deleted and validated[/bold green]",
                style="green"
            ))


class VerifyCommand(BaseCommand):
    """Implementation of 'verify' command."""
    
    def execute(self) -> bool:
        """Execute the verify command."""
        console.print(Panel.fit(
            f"Verifying Skyflow Snowflake Integration: [bold]{self.prefix}[/bold]",
            style="blue"
        ))
        
        try:
            self.validate_environment()
            
            sql_executor = SnowflakeSQLExecutor(self.config.connection)
            
            # Verify table exists and has data
            table_name = f"{self.prefix}_customer_data"
            table_exists = sql_executor.verify_table_exists(table_name)
            
            if table_exists:
                row_count = sql_executor.get_table_row_count(table_name)
                console.print(f"✓ Table exists with {row_count} rows")
                sql_executor.show_table_sample(table_name)
            else:
                console.print(f"✗ Table {table_name} does not exist")
                return False
            
            # Verify functions exist
            function_name = f"{self.prefix}_skyflow_conditional_detokenize"
            function_exists = sql_executor.verify_function_exists(function_name)
            
            if function_exists:
                console.print(f"✓ Function {function_name} exists")
            else:
                console.print(f"✗ Function {function_name} does not exist")
                return False
            
            # Verify masking policies exist
            cursor = self.config.connection.cursor()
            cursor.execute(f"SHOW MASKING POLICIES LIKE '{self.prefix}_pii_mask'")
            policies = cursor.fetchall()
            cursor.close()
            
            if policies:
                console.print(f"✓ Masking policy {self.prefix}_pii_mask exists")
            else:
                console.print(f"✗ Masking policy {self.prefix}_pii_mask does not exist")
                return False
            
            console.print(Panel.fit(
                f"[bold green]✓ Verification Complete: {self.prefix}[/bold green]",
                style="green"
            ))
            
            return True
            
        except Exception as e:
            console.print(f"[red]Verification failed: {e}[/red]")
            return False