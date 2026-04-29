"""
Test Checkpoint 3: stg_payments Model — Cents to Dollars Conversion

Validates that the student has:
- Created stg_payments.sql in models/staging/
- Used {{ source('jaffle_shop', 'payments') }} reference
- Applied cents-to-dollars conversion (division by 100)
- Renamed id to payment_id
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
def payments_sql(staging_dir):
    """Load stg_payments.sql content."""
    path = staging_dir / "stg_payments.sql"
    assert path.exists(), (
        "❌ stg_payments.sql not found\n"
        "   Expected: jaffle_shop_dbt/models/staging/stg_payments.sql\n"
        "   Did you create this file? (Section 3)"
    )
    with open(path, "r") as f:
        return f.read()


class TestStagingPayments:
    """Checkpoint 3: stg_payments model with type casting."""

    def test_stg_payments_file_exists(self, staging_dir):
        """stg_payments.sql must exist in the staging directory."""
        path = staging_dir / "stg_payments.sql"
        assert path.exists(), (
            "❌ stg_payments.sql not found\n"
            "   Expected: jaffle_shop_dbt/models/staging/stg_payments.sql\n"
            "   Did you create this file? (Section 3)"
        )

    def test_uses_source_reference(self, payments_sql):
        """stg_payments must reference the payments source table via {{ source() }}."""
        assert "{{ source(" in payments_sql, (
            "❌ stg_payments.sql must use {{ source('jaffle_shop', 'payments') }}\n"
            "   Did you reference the source correctly instead of hardcoding the table name?"
        )
        assert "payments" in payments_sql, (
            "❌ stg_payments.sql must reference the 'payments' source table\n"
            "   Expected: {{ source('jaffle_shop', 'payments') }}"
        )

    def test_has_select_statement(self, payments_sql):
        """stg_payments must contain a SELECT statement."""
        assert "SELECT" in payments_sql.upper(), (
            "❌ stg_payments.sql must contain a SELECT statement\n"
            "   Did you write the SQL query? (Section 3)"
        )

    def test_cents_to_dollars_conversion(self, payments_sql):
        """stg_payments must divide amount by 100 to convert cents to dollars."""
        content_lower = payments_sql.lower()
        has_division = "/ 100" in content_lower or "/100" in content_lower
        assert has_division, (
            "❌ stg_payments.sql must convert cents to dollars by dividing amount by 100\n"
            "   Expected: amount / 100.0 AS amount\n"
            "   Did you apply the cents-to-dollars conversion? (Section 3)"
        )

    def test_payment_id_rename(self, payments_sql):
        """stg_payments must rename id to payment_id."""
        assert "payment_id" in payments_sql.lower(), (
            "❌ stg_payments.sql must rename 'id' to 'payment_id'\n"
            "   Expected: id AS payment_id\n"
            "   Did you rename the primary key column? (Section 3)"
        )
