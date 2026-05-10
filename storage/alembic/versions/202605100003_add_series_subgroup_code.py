"""add series subgroup code

Revision ID: 202605100003
Revises: 202605100002
Create Date: 2026-05-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "202605100003"
down_revision: Union[str, None] = "202605100002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


SERIES_METADATA = [
    ("CBR:KEY_RATE", "ru_rates", "monetary_policy", "Bank of Russia key rate, the core monetary policy factor."),
    ("CBR:KEY_RATE_MEETING_DUMMY", "ru_rates", "monetary_policy_event", "Bank of Russia meeting dummy, capturing monetary policy event risk."),
    ("CBR:OFZ:ZCYC:10Y", "ru_rates", "ofz_curve", "Long end of the OFZ curve, reflecting risk premium and long-term expectations."),
    ("CBR:OFZ:ZCYC:1Y", "ru_rates", "ofz_curve", "Short end of the OFZ curve, reflecting Bank of Russia rate expectations and ruble yields."),
    ("CBR:OFZ:ZCYC:3Y", "ru_rates", "ofz_curve", "Middle segment of the OFZ curve, reflecting monetary policy expectations."),
    ("CBR:OFZ:ZCYC:5Y", "ru_rates", "ofz_curve", "Medium-term ruble yield from the OFZ zero-coupon curve."),
    ("CBR:RUBUSD", "target", "official_fx_rate", "Forecast target: the Bank of Russia official RUB/USD exchange rate."),
    ("ECONOMICS:USCOR", "energy_fundamentals", "rig_count", "U.S. crude oil rig count, an indicator of future oil supply."),
    ("ECONOMICS:USCOSC", "energy_fundamentals", "oil_stocks", "Change in U.S. crude oil inventories, a weekly fundamental oil shock."),
    ("ECONOMICS:USDFS", "energy_fundamentals", "distillate_stocks", "Change in distillate inventories, important for the diesel market."),
    ("ECONOMICS:USGSCH", "energy_fundamentals", "gasoline_stocks", "Change in U.S. gasoline inventories, an indicator of oil product demand and balance."),
    ("EXCHANGERATES:URALSUSD:DAILY", "energy", "oil_price_daily", "Daily Urals crude price, a fundamental oil factor for the ruble."),
    ("EXCHANGERATES:URALSUSD:LIVE15M", "energy", "oil_price_intraday", "Intraday update of the oil factor, useful for 15-minute nowcasting."),
    ("FRED:SOFR", "global_usd_risk", "usd_money_market", "Short-term U.S. dollar money market rate, a base measure of USD funding conditions."),
    ("FX_IDC:USDEUR", "global_usd_risk", "global_fx", "Indicator of broad U.S. dollar strength through the USD/EUR exchange rate."),
    ("FX_IDC:USDRUB", "fx_market", "market_usdrub_proxy", "Market USD/RUB proxy that can provide more timely information than the official Bank of Russia rate."),
    ("ICEENDEX:TFM1!", "energy", "gas_prices", "First TTF gas contract, an indicator of the European gas market."),
    ("ICEENDEX:TFM2!", "energy", "gas_prices", "Second TTF gas contract, used to monitor the shape of the gas forward curve."),
    ("ICEEUR:ULS1!", "energy", "oil_products", "Low sulphur gasoil futures, a proxy for oil products and refining margins."),
    ("MOEX:CNYRUB_TOM", "fx_market", "cnyrub_market", "Important market proxy for the ruble through CNY/RUB, especially when the role of USD/RUB on MOEX is limited."),
    ("MOEX:RTSI", "ru_market_risk", "equity_index_daily", "RTS Index, a proxy for Russian equity and risk sentiment."),
    ("MOEX:RTSI:15M", "ru_market_risk", "equity_index_intraday", "Intraday RTS Index, a timely Russian market risk sentiment indicator."),
    ("NYMEX:GZ1!", "energy", "oil_products", "NY Harbor ULSD financial futures, a proxy for the diesel market."),
    ("NYMEX:HO1!", "energy", "oil_products", "Heating oil and ULSD futures, an additional oil products indicator."),
    ("RUONIA_MAX_RATE", "ru_rates", "money_market_distribution", "Upper bound of RUONIA rates, reflecting stress or dispersion in the money market."),
    ("RUONIA_MIN_RATE", "ru_rates", "money_market_distribution", "Lower bound of RUONIA rates."),
    ("RUONIA_P25_RATE", "ru_rates", "money_market_distribution", "25th percentile of RUONIA rates, describing the shape of the rate distribution."),
    ("RUONIA_P75_RATE", "ru_rates", "money_market_distribution", "75th percentile of RUONIA rates, describing the shape of the rate distribution."),
    ("RUONIA_PARTICIPANTS_COUNT", "ru_rates", "money_market_activity", "Number of RUONIA participants, a measure of market depth."),
    ("RUONIA_RATE", "ru_rates", "money_market_rate", "Actual overnight ruble money market rate."),
    ("RUONIA_TRADES_COUNT", "ru_rates", "money_market_activity", "Number of RUONIA trades, a measure of money market activity."),
    ("RUONIA_VOLUME", "ru_rates", "money_market_liquidity", "RUONIA transaction volume, a proxy for money market liquidity."),
    ("RU_CPI_MOM", "ru_macro", "inflation", "Monthly inflation, affecting rate expectations and the real ruble yield."),
    ("RU_CPI_YOY", "ru_macro", "inflation", "Annual inflation, a fundamental macroeconomic indicator."),
    ("RU_CREDIT_CORPORATES", "ru_credit_money", "credit", "Corporate loans, a proxy for the credit cycle."),
    ("RU_CREDIT_HOUSEHOLDS", "ru_credit_money", "credit", "Household loans, a proxy for domestic demand and credit activity."),
    ("RU_FISCAL_FX_OPERATION_AMOUNT", "ru_fiscal", "fx_operations", "Finance Ministry and Bank of Russia FX and gold operations, a direct FX demand and supply factor."),
    ("RU_FISCAL_OILGAS_REVENUE", "ru_fiscal", "oilgas_revenue", "Federal oil and gas budget revenues, linking oil prices, the budget, and FX operations."),
    ("RU_INDUSTRIAL_PRODUCTION", "ru_macro", "real_activity", "Industrial production, an indicator of domestic economic activity."),
    ("RU_M2", "ru_credit_money", "money_supply", "M2 money supply, an indicator of ruble liquidity."),
    ("RU_TAX_ANY_DUE_DUMMY", "ru_tax_calendar", "any_tax_due", "Dummy for any tax payment due on date t."),
    ("RU_TAX_ANY_T0", "ru_tax_calendar", "any_tax_window", "Any tax payment on date t."),
    ("RU_TAX_ANY_T_MINUS_1", "ru_tax_calendar", "any_tax_window", "One business day before any tax payment."),
    ("RU_TAX_ANY_T_MINUS_2", "ru_tax_calendar", "any_tax_window", "Two business days before any tax payment."),
    ("RU_TAX_ANY_T_MINUS_3", "ru_tax_calendar", "any_tax_window", "Three business days before any tax payment."),
    ("RU_TAX_ANY_WINDOW_T_MINUS_3_TO_T", "ru_tax_calendar", "any_tax_window", "Window from t-3 to t before any tax payment."),
    ("RU_TAX_DUE_COUNT", "ru_tax_calendar", "tax_due_intensity", "Number of tax payments due on the date, measuring tax pressure intensity."),
    ("RU_TAX_NDPI_DUE_DUMMY", "ru_tax_calendar", "ndpi", "Mineral extraction tax payment due date dummy."),
    ("RU_TAX_NDPI_T0", "ru_tax_calendar", "ndpi_window", "Mineral extraction tax payment on date t."),
    ("RU_TAX_NDPI_T_MINUS_1", "ru_tax_calendar", "ndpi_window", "One business day before mineral extraction tax payment."),
    ("RU_TAX_NDPI_T_MINUS_2", "ru_tax_calendar", "ndpi_window", "Two business days before mineral extraction tax payment."),
    ("RU_TAX_NDPI_T_MINUS_3", "ru_tax_calendar", "ndpi_window", "Three business days before mineral extraction tax payment."),
    ("RU_TAX_NDPI_WINDOW_T_MINUS_3_TO_T", "ru_tax_calendar", "ndpi_window", "Window from t-3 to t before mineral extraction tax payment."),
    ("RU_TAX_PROFIT_DUE_DUMMY", "ru_tax_calendar", "profit_tax", "Profit tax payment due date dummy."),
    ("RU_TAX_PROFIT_T0", "ru_tax_calendar", "profit_tax_window", "Profit tax payment on date t."),
    ("RU_TAX_PROFIT_T_MINUS_1", "ru_tax_calendar", "profit_tax_window", "One business day before profit tax payment."),
    ("RU_TAX_PROFIT_T_MINUS_2", "ru_tax_calendar", "profit_tax_window", "Two business days before profit tax payment."),
    ("RU_TAX_PROFIT_T_MINUS_3", "ru_tax_calendar", "profit_tax_window", "Three business days before profit tax payment."),
    ("RU_TAX_PROFIT_WINDOW_T_MINUS_3_TO_T", "ru_tax_calendar", "profit_tax_window", "Window from t-3 to t before profit tax payment."),
    ("RU_TAX_QTR_END_LAST_BUSINESS_DAY", "ru_tax_calendar", "quarter_end", "Last business day of the quarter, capturing possible balancing or liquidity effects."),
    ("RU_TAX_QTR_END_WINDOW_T_MINUS_3_TO_T", "ru_tax_calendar", "quarter_end_window", "Window from t-3 to t before quarter end."),
    ("RU_TAX_QTR_PAYMENT_DUE_DUMMY", "ru_tax_calendar", "quarter_tax_payment", "Quarterly tax payment due date dummy."),
    ("RU_TAX_QTR_PAYMENT_T0", "ru_tax_calendar", "quarter_tax_payment_window", "Quarterly tax payment on date t."),
    ("RU_TAX_QTR_PAYMENT_T_MINUS_1", "ru_tax_calendar", "quarter_tax_payment_window", "One business day before quarterly tax payment."),
    ("RU_TAX_QTR_PAYMENT_T_MINUS_2", "ru_tax_calendar", "quarter_tax_payment_window", "Two business days before quarterly tax payment."),
    ("RU_TAX_QTR_PAYMENT_T_MINUS_3", "ru_tax_calendar", "quarter_tax_payment_window", "Three business days before quarterly tax payment."),
    ("RU_TAX_QTR_PAYMENT_WINDOW_T_MINUS_3_TO_T", "ru_tax_calendar", "quarter_tax_payment_window", "Window from t-3 to t before quarterly tax payments."),
    ("RU_TAX_RU_BUSINESS_DAY", "ru_tax_calendar", "business_day", "Russian business day dummy, used for calendar alignment and lag construction."),
    ("RU_TAX_VAT_DUE_DUMMY", "ru_tax_calendar", "vat", "VAT payment due date dummy."),
    ("RU_TAX_VAT_T0", "ru_tax_calendar", "vat_window", "VAT payment on date t."),
    ("RU_TAX_VAT_T_MINUS_1", "ru_tax_calendar", "vat_window", "One business day before VAT payment."),
    ("RU_TAX_VAT_T_MINUS_2", "ru_tax_calendar", "vat_window", "Two business days before VAT payment."),
    ("RU_TAX_VAT_T_MINUS_3", "ru_tax_calendar", "vat_window", "Three business days before VAT payment."),
    ("RU_TAX_VAT_WINDOW_T_MINUS_3_TO_T", "ru_tax_calendar", "vat_window", "Window from t-3 to t before VAT payment."),
    ("RU_TAX_WEEK_DUMMY", "ru_tax_calendar", "tax_week", "Tax week dummy, a broad calendar factor for ruble demand."),
    ("TVC:DXY", "global_usd_risk", "usd_index", "U.S. Dollar Index, a key external pressure factor for RUB/USD."),
    ("TVC:US02Y", "global_usd_risk", "us_rates", "2-year U.S. Treasury yield, sensitive to Federal Reserve expectations."),
    ("TVC:US10Y", "global_usd_risk", "us_rates", "10-year U.S. Treasury yield, a global rates and risk factor."),
]


def upgrade() -> None:
    op.add_column("series", sa.Column("subgroup_code", sa.Text(), nullable=True))
    for series_code, group_code, subgroup_code, description in SERIES_METADATA:
        op.execute(
            f"""
            UPDATE series
            SET group_code = {_sql_literal(group_code)},
                subgroup_code = {_sql_literal(subgroup_code)},
                description = {_sql_literal(description)}
            WHERE series_code = {_sql_literal(series_code)}
            """
        )


def downgrade() -> None:
    op.drop_column("series", "subgroup_code")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
