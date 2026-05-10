import json
from pathlib import Path
from typing import Callable

from ingestion.adapters.base import BaseAdapter
from ingestion.adapters.cbr import CbrAdapter
from ingestion.adapters.cbr_secinfo import CbrSecInfoAdapter
from ingestion.adapters.cbr_trade_balance import CbrTradeBalanceAdapter
from ingestion.adapters.eia import EiaAdapter
from ingestion.adapters.exchangerates import ExchangeRatesAdapter
from ingestion.adapters.fred import FredAdapter, FredSofrAdapter
from ingestion.adapters.manual_csv import ManualCsvAdapter
from ingestion.adapters.minfin_oilgas import MinfinOilGasAdapter
from ingestion.adapters.tradingview import TradingViewAdapter
from ingestion.adapters.moex import MoexAdapter
from ingestion.adapters.rosstat_industrial import RosstatIndustrialAdapter
from ingestion.adapters.rosstat_retail_sales import RosstatRetailSalesAdapter
from ingestion.adapters.ru_tax_dummy_calendar import RuTaxDummyCalendarAdapter
from ingestion.adapters.web import WebPageAdapter
from ingestion.adapters.yahoo import YahooAdapter
from ingestion.schemas.sources import SourceDefinition


AdapterFactory = Callable[[], BaseAdapter]


class SourceRegistry:
    def __init__(self) -> None:
        self._sources: dict[str, SourceDefinition] = {}
        self._adapters: dict[str, AdapterFactory] = {}
        self.register_adapter(WebPageAdapter.name, WebPageAdapter)
        self.register_adapter(CbrAdapter.name, CbrAdapter)
        self.register_adapter(CbrSecInfoAdapter.name, CbrSecInfoAdapter)
        self.register_adapter(CbrTradeBalanceAdapter.name, CbrTradeBalanceAdapter)
        self.register_adapter(MoexAdapter.name, MoexAdapter)
        self.register_adapter(EiaAdapter.name, EiaAdapter)
        self.register_adapter(FredAdapter.name, FredAdapter)
        self.register_adapter(FredSofrAdapter.name, FredSofrAdapter)
        self.register_adapter(ExchangeRatesAdapter.name, ExchangeRatesAdapter)
        self.register_adapter(YahooAdapter.name, YahooAdapter)
        self.register_adapter(ManualCsvAdapter.name, ManualCsvAdapter)
        self.register_adapter(MinfinOilGasAdapter.name, MinfinOilGasAdapter)
        self.register_adapter(RosstatIndustrialAdapter.name, RosstatIndustrialAdapter)
        self.register_adapter(RosstatRetailSalesAdapter.name, RosstatRetailSalesAdapter)
        self.register_adapter(RuTaxDummyCalendarAdapter.name, RuTaxDummyCalendarAdapter)
        self.register_adapter(TradingViewAdapter.name, TradingViewAdapter)

    def register_adapter(self, name: str, factory: AdapterFactory) -> None:
        self._adapters[name] = factory

    def register_source(self, source: SourceDefinition) -> None:
        self._sources[source.source_code] = source

    def get_source(self, source_code: str) -> SourceDefinition:
        try:
            return self._sources[source_code]
        except KeyError as exc:
            raise KeyError(f"source is not registered: {source_code}") from exc

    def list_sources(self, *, enabled_only: bool = False) -> list[SourceDefinition]:
        sources = list(self._sources.values())
        if enabled_only:
            return [source for source in sources if source.enabled]
        return sources

    def build_adapter(self, source: SourceDefinition) -> BaseAdapter:
        try:
            return self._adapters[source.adapter_name]()
        except KeyError as exc:
            raise KeyError(f"adapter is not registered: {source.adapter_name}") from exc

    def load_json(self, path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_sources = payload if isinstance(payload, list) else payload.get("sources", [])
        for raw_source in raw_sources:
            if isinstance(raw_source, dict) and "_comment" in raw_source and "source_code" not in raw_source:
                continue
            self.register_source(SourceDefinition.model_validate(raw_source))
