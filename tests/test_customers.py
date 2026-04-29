"""
Test Checkpoint 1: stg_customers Model

Validates that the student has:
- Created staging/ directory
- Created stg_customers.sql with correct naming
- Used {{ source() }} reference
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
    staging_path = project_dir / "models" / "staging"
    return staging_path if staging_path.exists() else None


class TestCheckpoint1:
    """Checkpoint 1: stg_customers staging model created."""

    def test_staging_directory_exists(self, staging_dir):
        """Must have created staging/ directory."""
        assert staging_dir is not None, (
            "❌ No staging directory found\n"
            "   Expected: jaffle_shop_dbt/models/staging/\n"
            "   Did you copy your dbt project correctly? (Section 0)\n"
            "   Run: cd jaffle_shop_dbt && mkdir -p models/staging"
        )

    def test_stg_customers_file_exists(self, staging_dir):
        """stg_customers.sql must exist in staging directory."""
        if staging_dir is None:
            pytest.skip("Staging directory doesn't exist yet")

        customers_file = staging_dir / "stg_customers.sql"
        assert customers_file.exists(), (
            "❌ stg_customers.sql not found\n"
            "   Expected: jaffle_shop_dbt/models/staging/stg_customers.sql\n"
            "   Did you create this file? (Section 1)"
        )

    def test_stg_customers_uses_source(self, staging_dir):
        """stg_customers must use {{ source() }} function."""
        if staging_dir is None:
            pytest.skip("Staging directory doesn't exist yet")

        customers_file = staging_dir / "stg_customers.sql"
        if not customers_file.exists():
            pytest.skip("stg_customers.sql doesn't exist yet")

        with open(customers_file, "r") as f:
            content = f.read()

        assert "{{ source(" in content, (
            "❌ stg_customers.sql must use {{ source() }} function\n"
            "   Expected: SELECT * FROM {{ source('jaffle_shop', 'customers') }}\n"
            "   Did you reference the source correctly? (Section 1)"
        )

    def test_stg_customers_renames_id(self, staging_dir):
        """stg_customers must rename id to customer_id."""
        if staging_dir is None:
            pytest.skip("Staging directory doesn't exist yet")

        customers_file = staging_dir / "stg_customers.sql"
        if not customers_file.exists():
            pytest.skip("stg_customers.sql doesn't exist yet")

        with open(customers_file, "r") as f:
            content = f.read().lower()

        assert "customer_id" in content, (
            "❌ stg_customers.sql must rename 'id' to 'customer_id'\n"
            "   Expected: id AS customer_id\n"
            "   Did you rename the primary key column? (Section 1)"
        )

    def test_stg_customers_has_select(self, staging_dir):
        """stg_customers must have a SELECT statement."""
        if staging_dir is None:
            pytest.skip("Staging directory doesn't exist yet")

        customers_file = staging_dir / "stg_customers.sql"
        if not customers_file.exists():
            pytest.skip("stg_customers.sql doesn't exist yet")

        with open(customers_file, "r") as f:
            content = f.read().upper()

        assert "SELECT" in content, (
            "❌ stg_customers.sql must contain a SELECT statement\n"
            "   Did you add the SQL query? (Section 1)"
        )
