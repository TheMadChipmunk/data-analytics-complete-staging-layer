"""
Test Checkpoint 2: stg_orders Model

Validates that the student has:
- Created stg_orders.sql in models/staging/
- Used {{ source('jaffle_shop', 'orders') }} reference
- Renamed id → order_id and user_id → customer_id
- Added SELECT statement
"""

import pytest
from pathlib import Path


@pytest.fixture
def project_dir():
    """Get jaffle_shop_dbt directory within challenge repo."""
    challenge_dir = Path(__file__).parent.parent
    dbt_project_dir = challenge_dir / "jaffle_shop_dbt"

    assert dbt_project_dir.exists(), (
        "❌ jaffle_shop_dbt/ directory not found\n"
        "   Did you copy your dbt project from the previous challenge? (Section 0)\n"
        "   Run: ls .. to find the previous challenge directory, then:\n"
        "   cp -rP ../PREVIOUS-CHALLENGE/jaffle_shop_dbt ."
    )

    return dbt_project_dir


@pytest.fixture
def staging_dir(project_dir):
    """Get the staging models directory."""
    path = project_dir / "models" / "staging"
    assert path.exists(), (
        "❌ models/staging/ directory not found\n"
        "   Did you copy your dbt project correctly? (Section 0)"
    )
    return path


@pytest.fixture
def orders_sql(staging_dir):
    """Load stg_orders.sql content."""
    path = staging_dir / "stg_orders.sql"
    assert path.exists(), (
        "❌ stg_orders.sql not found\n"
        "   Expected: jaffle_shop_dbt/models/staging/stg_orders.sql\n"
        "   Did you create this file? (Section 2)"
    )
    with open(path, "r") as f:
        return f.read()


class TestCheckpoint2:
    """Checkpoint 2: stg_orders staging model created."""

    def test_stg_orders_file_exists(self, staging_dir):
        """stg_orders.sql must exist in the staging directory."""
        path = staging_dir / "stg_orders.sql"
        assert path.exists(), (
            "❌ stg_orders.sql not found\n"
            "   Expected: jaffle_shop_dbt/models/staging/stg_orders.sql\n"
            "   Did you create this file? (Section 2)"
        )

    def test_uses_source_reference(self, orders_sql):
        """stg_orders must reference the orders source table via {{ source() }}."""
        assert "{{ source(" in orders_sql, (
            "❌ stg_orders.sql must use {{ source('jaffle_shop', 'orders') }}\n"
            "   Did you reference the source correctly instead of hardcoding the table name?"
        )
        assert "orders" in orders_sql, (
            "❌ stg_orders.sql must reference the 'orders' source table\n"
            "   Expected: {{ source('jaffle_shop', 'orders') }}"
        )

    def test_has_select_statement(self, orders_sql):
        """stg_orders must contain a SELECT statement."""
        assert "SELECT" in orders_sql.upper(), (
            "❌ stg_orders.sql must contain a SELECT statement\n"
            "   Did you write the SQL query? (Section 2)"
        )

    def test_renames_id_to_order_id(self, orders_sql):
        """stg_orders must rename id to order_id."""
        assert "order_id" in orders_sql.lower(), (
            "❌ stg_orders.sql must rename 'id' to 'order_id'\n"
            "   Expected: id AS order_id\n"
            "   Did you rename the primary key column? (Section 2)"
        )

    def test_renames_user_id_to_customer_id(self, orders_sql):
        """stg_orders must rename user_id to customer_id."""
        assert "customer_id" in orders_sql.lower(), (
            "❌ stg_orders.sql must rename 'user_id' to 'customer_id'\n"
            "   Expected: user_id AS customer_id\n"
            "   This standardises foreign key naming across all models. (Section 2)"
        )
