"""
Test Checkpoint 4: Documentation Added

Validates that the student has:
- Updated schema.yml with model documentation
- Added descriptions for all three staging models
- Added column-level descriptions
"""

import pytest
from pathlib import Path
import yaml


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
def schema_file(project_dir):
    """Get schema.yml file path — check common locations."""
    models_dir = project_dir / "models"

    possible_locations = [
        models_dir / "schema.yml",
        models_dir / "staging" / "schema.yml",
    ]

    for path in possible_locations:
        if path.exists():
            return path

    return None


@pytest.fixture
def schema_content(schema_file):
    """Load schema.yml content."""
    if schema_file is None:
        return None

    with open(schema_file, "r") as f:
        return yaml.safe_load(f)


class TestCheckpoint4:
    """Checkpoint 4: Documentation added for all staging models."""

    def test_schema_file_exists(self, schema_file):
        """schema.yml must exist."""
        assert schema_file is not None, (
            "❌ schema.yml not found\n"
            "   Expected locations:\n"
            "   - jaffle_shop_dbt/models/schema.yml\n"
            "   - jaffle_shop_dbt/models/staging/schema.yml\n"
            "   Did you create or update this file? (Section 4)"
        )

    def test_schema_has_models_section(self, schema_content):
        """schema.yml must have a 'models' section."""
        if schema_content is None:
            pytest.skip("schema.yml doesn't exist yet")

        assert "models" in schema_content, (
            "❌ schema.yml must have a 'models:' section\n"
            "   Did you add model documentation? (Section 4)"
        )

    def test_all_staging_models_documented(self, schema_content):
        """All three staging models should be documented."""
        if schema_content is None:
            pytest.skip("schema.yml doesn't exist yet")

        if "models" not in schema_content:
            pytest.skip("No models section in schema.yml yet")

        models = schema_content.get("models", [])
        model_names = [m.get("name", "") for m in models]

        required_models = ["stg_customers", "stg_orders", "stg_payments"]
        missing = [m for m in required_models if m not in model_names]

        assert len(missing) == 0, (
            "❌ Missing documentation for staging models:\n"
            + "\n".join([f"   - {name}" for name in missing])
            + "\n   Did you add all three models to the 'models:' section? (Section 4)"
        )

    def test_models_have_descriptions(self, schema_content):
        """All documented staging models should have descriptions."""
        if schema_content is None:
            pytest.skip("schema.yml doesn't exist yet")

        if "models" not in schema_content:
            pytest.skip("No models section in schema.yml yet")

        models = schema_content.get("models", [])
        staging_models = [m for m in models if m.get("name", "").startswith("stg_")]

        missing_descriptions = [
            m.get("name", "unknown")
            for m in staging_models
            if not m.get("description")
        ]

        assert len(missing_descriptions) == 0, (
            "❌ Models missing descriptions:\n"
            + "\n".join([f"   - {name}" for name in missing_descriptions])
            + "\n   Each model should have a description explaining its purpose"
        )

    def test_models_have_column_docs(self, schema_content):
        """Staging models should have column-level documentation."""
        if schema_content is None:
            pytest.skip("schema.yml doesn't exist yet")

        if "models" not in schema_content:
            pytest.skip("No models section in schema.yml yet")

        models = schema_content.get("models", [])
        staging_models = [m for m in models if m.get("name", "").startswith("stg_")]

        missing_columns = [
            m.get("name", "unknown")
            for m in staging_models
            if not m.get("columns")
        ]

        assert len(missing_columns) == 0, (
            "❌ Models missing column documentation:\n"
            + "\n".join([f"   - {name}" for name in missing_columns])
            + "\n   Add a 'columns:' section with column descriptions. (Section 4)\n"
            "   Example:\n"
            "   columns:\n"
            "     - name: customer_id\n"
            "       description: Unique customer identifier"
        )

    def test_sources_section_preserved(self, schema_content):
        """The sources: section from previous challenges must still be present in schema.yml."""
        if schema_content is None:
            pytest.skip("schema.yml doesn't exist yet")

        assert "sources" in schema_content, (
            "❌ The 'sources:' section is missing from schema.yml\n"
            "   Did you accidentally overwrite the whole file instead of adding to it?\n"
            "   schema.yml should contain BOTH a 'sources:' section (from Challenge 03)\n"
            "   AND a 'models:' section (from this challenge).\n"
            "   Check your file and restore the sources block if it was deleted."
        )
