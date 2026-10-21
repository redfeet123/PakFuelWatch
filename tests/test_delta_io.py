"""Tests for Delta Lake idempotent merge and read utilities."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ingestion.core.delta_io import read_delta, read_delta_pandas, write_delta_merge


def test_delta_write_merge_idempotency(tmp_path: Path) -> None:
    """Prove that writing the exact same data twice leaves the row count unchanged."""
    table_path = str(tmp_path / "silver" / "brent_daily")

    initial_df = pd.DataFrame(
        [
            {"date": date(2024, 1, 1), "price_usd": 77.5, "source": "eia"},
            {"date": date(2024, 1, 2), "price_usd": 78.2, "source": "eia"},
            {"date": date(2024, 1, 3), "price_usd": 79.0, "source": "eia"},
        ]
    )

    # 1. Initial write
    write_delta_merge(initial_df, table_path=table_path, merge_keys=["date", "source"])

    first_read = read_delta(table_path)
    assert len(first_read) == 3
    assert first_read.num_rows == 3

    # 2. Write the exact same data a second time
    write_delta_merge(initial_df, table_path=table_path, merge_keys=["date", "source"])

    second_read = read_delta(table_path)
    # PROOF: Row count is strictly unchanged after writing identical data
    assert len(second_read) == 3
    assert second_read.num_rows == 3

    # 3. Update an existing row and append a new row
    updated_df = pd.DataFrame(
        [
            # Existing row with updated price
            {"date": date(2024, 1, 3), "price_usd": 80.5, "source": "eia"},
            # New row
            {"date": date(2024, 1, 4), "price_usd": 81.1, "source": "eia"},
        ]
    )

    write_delta_merge(updated_df, table_path=table_path, merge_keys=["date", "source"])

    third_read = read_delta_pandas(table_path).sort_values("date").reset_index(drop=True)
    assert len(third_read) == 4

    # Verify that the existing row updated cleanly
    updated_row = third_read[third_read["date"] == date(2024, 1, 3)].iloc[0]
    assert updated_row["price_usd"] == 80.5


def test_delta_merge_keys_validation(tmp_path: Path) -> None:
    """Verify that empty merge_keys raises a ValueError."""
    table_path = str(tmp_path / "test_table")
    df = pd.DataFrame([{"id": 1, "value": "test"}])

    with pytest.raises(ValueError, match="merge_keys must contain at least one column"):
        write_delta_merge(df, table_path=table_path, merge_keys=[])
