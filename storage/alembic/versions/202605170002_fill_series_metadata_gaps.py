"""fill series metadata gaps

Revision ID: 202605170002
Revises: 202605170001
Create Date: 2026-05-17 00:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "202605170002"
down_revision: Union[str, None] = "202605170001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SERIES_METADATA = [
    (
        "TVC:US10Y-US02Y",
        "15min",
        "percentage_points",
        "global_usd_risk",
        "us_rates",
        "U.S. Treasury 10Y minus 2Y yield spread, a market measure of the U.S. yield curve and global rate expectations.",
    ),
    (
        "MOEX:IMOEX",
        "daily",
        "index_points",
        "ru_market_risk",
        "equity_index_daily",
        "MOEX Russia Index daily close, a broad Russian equity market and domestic risk sentiment factor.",
    ),
    (
        "MOEX:IMOEX:15M",
        "15min",
        "index_points",
        "ru_market_risk",
        "equity_index_intraday",
        "Intraday MOEX Russia Index, a timely Russian equity market and domestic risk sentiment factor.",
    ),
    (
        "CBR:BLIQUIDITY:STR_LI_DEF",
        "daily",
        "billion RUB",
        "ru_rates",
        "banking_liquidity",
        "Bank of Russia structural liquidity deficit or surplus, a core measure of banking system liquidity conditions.",
    ),
    (
        "CBR:BLIQUIDITY:STR_LI_DEF_NEW",
        "daily",
        "billion RUB",
        "ru_rates",
        "banking_liquidity",
        "Bank of Russia structural liquidity deficit or surplus under the new methodology, tracking ruble liquidity conditions.",
    ),
    (
        "CBR:BLIQUIDITY:CORR_ACC",
        "daily",
        "billion RUB",
        "ru_rates",
        "banking_liquidity",
        "Credit institutions correspondent account balances with the Bank of Russia, a direct banking liquidity indicator.",
    ),
    (
        "CBR:BLIQUIDITY:AVG_RR",
        "daily",
        "billion RUB",
        "ru_rates",
        "banking_liquidity",
        "Average required reserves of credit institutions, a banking liquidity and reserve requirement indicator.",
    ),
    (
        "RU_CPI_YOY",
        "monthly",
        "percent",
        "ru_macro",
        "inflation",
        "Annual inflation, a fundamental macroeconomic indicator.",
    ),
    (
        "RU_INDUSTRIAL_PRODUCTION",
        "monthly",
        "percent",
        "ru_macro",
        "real_activity",
        "Industrial production, an indicator of domestic economic activity.",
    ),
    (
        "RU_FISCAL_FX_OPERATION_AMOUNT",
        "monthly",
        "billion RUB",
        "ru_fiscal",
        "fx_operations",
        "Finance Ministry and Bank of Russia FX and gold operations, a direct FX demand and supply factor.",
    ),
    (
        "RU_FISCAL_OILGAS_REVENUE",
        "monthly",
        "billion RUB",
        "ru_fiscal",
        "oilgas_revenue",
        "Federal oil and gas budget revenues, linking oil prices, the budget, and FX operations.",
    ),
    (
        "RU_FISCAL_ADDITIONAL_OILGAS_REVENUE",
        "monthly",
        "billion RUB",
        "ru_fiscal",
        "oilgas_revenue",
        "Additional oil and gas budget revenues from the Minfin oil/gas revenue table.",
    ),
]


TAX_SERIES_UNITS = {
    "RU_TAX_DUE_COUNT": "count",
}

TAX_SERIES_CODES = [
    "RU_TAX_ANY_DUE_DUMMY",
    "RU_TAX_ANY_T0",
    "RU_TAX_ANY_T_MINUS_1",
    "RU_TAX_ANY_T_MINUS_2",
    "RU_TAX_ANY_T_MINUS_3",
    "RU_TAX_ANY_WINDOW_T_MINUS_3_TO_T",
    "RU_TAX_DUE_COUNT",
    "RU_TAX_NDPI_DUE_DUMMY",
    "RU_TAX_NDPI_T0",
    "RU_TAX_NDPI_T_MINUS_1",
    "RU_TAX_NDPI_T_MINUS_2",
    "RU_TAX_NDPI_T_MINUS_3",
    "RU_TAX_NDPI_WINDOW_T_MINUS_3_TO_T",
    "RU_TAX_PROFIT_DUE_DUMMY",
    "RU_TAX_PROFIT_T0",
    "RU_TAX_PROFIT_T_MINUS_1",
    "RU_TAX_PROFIT_T_MINUS_2",
    "RU_TAX_PROFIT_T_MINUS_3",
    "RU_TAX_PROFIT_WINDOW_T_MINUS_3_TO_T",
    "RU_TAX_QTR_END_LAST_BUSINESS_DAY",
    "RU_TAX_QTR_END_WINDOW_T_MINUS_3_TO_T",
    "RU_TAX_QTR_PAYMENT_DUE_DUMMY",
    "RU_TAX_QTR_PAYMENT_T0",
    "RU_TAX_QTR_PAYMENT_T_MINUS_1",
    "RU_TAX_QTR_PAYMENT_T_MINUS_2",
    "RU_TAX_QTR_PAYMENT_T_MINUS_3",
    "RU_TAX_QTR_PAYMENT_WINDOW_T_MINUS_3_TO_T",
    "RU_TAX_RU_BUSINESS_DAY",
    "RU_TAX_VAT_DUE_DUMMY",
    "RU_TAX_VAT_T0",
    "RU_TAX_VAT_T_MINUS_1",
    "RU_TAX_VAT_T_MINUS_2",
    "RU_TAX_VAT_T_MINUS_3",
    "RU_TAX_VAT_WINDOW_T_MINUS_3_TO_T",
    "RU_TAX_WEEK_DUMMY",
]


def upgrade() -> None:
    for series_code, frequency, units, group_code, subgroup_code, description in SERIES_METADATA:
        op.execute(
            f"""
            UPDATE series
            SET frequency = {_sql_literal(frequency)},
                units = {_sql_literal(units)},
                group_code = {_sql_literal(group_code)},
                subgroup_code = {_sql_literal(subgroup_code)},
                description = {_sql_literal(description)}
            WHERE series_code = {_sql_literal(series_code)}
            """
        )

    for series_code in TAX_SERIES_CODES:
        op.execute(
            f"""
            UPDATE series
            SET frequency = 'daily',
                units = {_sql_literal(TAX_SERIES_UNITS.get(series_code, "dummy"))}
            WHERE series_code = {_sql_literal(series_code)}
              AND (frequency IS NULL OR units IS NULL)
            """
        )


def downgrade() -> None:
    pass


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
