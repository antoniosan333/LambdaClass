from __future__ import annotations

from lambdaclass.data_adapters.optionsdx_quality import apply_quality_rules, rows_to_cleaned_dataframe


def test_negative_iv_nulled_and_flagged() -> None:
    row = {
        "quote_date": "2012-01-03",
        "quote_unixtime": 1325624400,
        "bid": 10.0,
        "ask": 11.0,
        "iv": -0.05,
    }
    out = apply_quality_rules(row)
    assert out["iv"] is None
    assert "NEGATIVE_IV" in out["quality_flags"]


def test_crossed_market_nulls_bid_ask() -> None:
    row = {
        "quote_date": "2021-06-01",
        "quote_unixtime": 1622520046,
        "bid": 0.101,
        "ask": 0.0505,
        "iv": 0.5,
    }
    out = apply_quality_rules(row)
    assert out["bid"] is None and out["ask"] is None
    assert out["is_crossed_market"] is True
    assert "CROSSED_MARKET" in out["quality_flags"]


def test_rows_to_cleaned_dataframe_has_expected_columns() -> None:
    rows = [
        {
            "quote_date": "2012-01-03",
            "quote_unixtime": 1325624400,
            "bid": 1.0,
            "ask": 2.0,
            "iv": 0.3,
        }
    ]
    df = rows_to_cleaned_dataframe(rows)
    assert "mid" in df.columns and "spread" in df.columns
    assert df.iloc[0]["mid"] == 1.5
    assert df.iloc[0]["spread"] == 1.0
