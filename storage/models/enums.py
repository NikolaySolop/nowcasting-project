from enum import Enum


class SourceType(str, Enum):
    API = "api"
    CSV = "csv"
    MANUAL = "manual"
    WEB = "web"
    VENDOR = "vendor"
    EXCHANGE = "exchange"

class Frequency(str, Enum):
    MIN_15 = "15min"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ANNUAL = "annual"


class TransformType(str, Enum):
    LEVEL = "level"
    LOG_RETURN = "log_return"
    DIFF = "diff"
    SPREAD = "spread"
    YOY = "yoy"
    MOM = "mom"
