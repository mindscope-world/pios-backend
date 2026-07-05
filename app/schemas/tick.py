from dataclasses import dataclass
from datetime import datetime

@dataclass
class Tick:
    symbol: str
    price: float
    volume: float
    asset_class: str   # crypto | stock | forex
    source: str        # binance | alpaca | forex
    timestamp: str