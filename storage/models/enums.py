from enum import Enum


class SourceType(str, Enum):
    API = "api"
    CSV = "csv"
    MANUAL = "manual"
    WEB = "web"
    VENDOR = "vendor"
    EXCHANGE = "exchange"

class BlockCode(str, enum.Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"
    G = "G"
    H = "H"
    I = "I"


class Frequency(str, enum.Enum):
    MIN_15 = "15min"
    MIN_30 = "30min"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    EVENT = "event"
    MODEL_STEP = "model_step"


class AssetClass(str, enum.Enum):
    FX = "fx"
    RATES = "rates"
    OIL = "oil"
    MACRO = "macro"
    EVENT = "event"
    TAX = "tax"
    SANCTIONS = "sanctions"


class TransformType(str, enum.Enum):
    LEVEL = "level"
    LOG_RETURN = "log_return"
    DIFF = "diff"
    SPREAD = "spread"
    YOY = "yoy"
    MOM = "mom"
