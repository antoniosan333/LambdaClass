from __future__ import annotations

from pathlib import Path

import pytest

from lambdaclass.data_adapters.optionsdx_parser import (
    detect_schema_column_count,
    parse_optionsdx_file,
    parse_size_field,
)


def test_parse_size_field() -> None:
    assert parse_size_field("5 x 10") == (5, 10)
    assert parse_size_field("0 x 699") == (0, 699)
    assert parse_size_field("") == (None, None)
    assert parse_size_field(None) == (None, None)


def test_detect_schema_column_count() -> None:
    h33 = "[QUOTE_UNIXTIME], [QUOTE_READTIME], [QUOTE_DATE], [QUOTE_TIME_HOURS], [UNDERLYING_LAST], [EXPIRE_DATE], [EXPIRE_UNIX], [DTE], [C_DELTA], [C_GAMMA], [C_VEGA], [C_THETA], [C_RHO], [C_IV], [C_VOLUME], [C_LAST], [C_SIZE], [C_BID], [C_ASK], [STRIKE], [P_BID], [P_ASK], [P_SIZE], [P_LAST], [P_DELTA], [P_GAMMA], [P_VEGA], [P_THETA], [P_RHO], [P_IV], [P_VOLUME], [STRIKE_DISTANCE], [STRIKE_DISTANCE_PCT]"
    assert detect_schema_column_count(h33) == 33
    h28 = "[QUOTE_UNIXTIME], [QUOTE_READTIME], [QUOTE_DATE], [QUOTE_TIME_HOURS], [INSTRUMENT_NAME], [BASE_CURRENCY], [CONTRACT_SIZE], [UNDERLYING_INDEX], [UNDERLYING_PRICE], [EXPIRY_DATE], [EXPIRY_UNIX], [EXPIRY_TIME], [DTE], [OPTION_RIGHT], [STRIKE], [BID_SIZE], [BID_PRICE], [ASK_PRICE], [ASK_SIZE], [OPEN_INTEREST], [VOLUME], [DELTA], [GAMMA], [VEGA], [THETA], [RHO], [MARK_IV], [STRIKE_DISTANCE_PCT]"
    assert detect_schema_column_count(h28) == 28


def test_parse_fixture_33_expands_to_two_sides() -> None:
    path = Path(__file__).parent / "fixtures" / "optionsdx" / "spy_eod_201201" / "spy_eod_201201.txt"
    res = parse_optionsdx_file(path)
    assert res.schema == "33"
    assert res.raw_line_count == 1
    assert len(res.rows) == 2
    call = next(r for r in res.rows if r["side"] == "call")
    assert call["symbol"] == "SPY"
    assert call["strike"] == 115.0
    assert call["bid_size"] == 395 and call["ask_size"] == 30


def test_parse_fixture_28_single_row() -> None:
    path = Path(__file__).parent / "fixtures" / "optionsdx" / "btc_eod_202106" / "btc_eod_202106.txt"
    res = parse_optionsdx_file(path)
    assert res.schema == "28"
    assert len(res.rows) == 1
    assert res.rows[0]["symbol"] == "BTC"
    assert res.rows[0]["contract_symbol"] == "BTC-1JUN21-34000-C"


@pytest.mark.parametrize(
    "folder,stem",
    [
        ("spy_eod_201201", "spy_eod_201201"),
        ("btc_eod_202106", "btc_eod_202106"),
    ],
)
def test_parse_errors_empty_for_fixtures(folder: str, stem: str) -> None:
    path = Path(__file__).parent / "fixtures" / "optionsdx" / folder / f"{stem}.txt"
    res = parse_optionsdx_file(path)
    assert res.parse_errors == []
