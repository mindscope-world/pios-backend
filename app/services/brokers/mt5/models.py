from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class MT5ConnectPayload(BaseModel):
    account_number: str
    password: str
    server: str
    is_paper: bool = True
    connection_name: Optional[str] = None

class MT5OrderRequest(BaseModel):
    action: str = "ORDER_TYPE_BUY"  # BUY, SELL, PENDING, CLOSE
    symbol: str
    volume: float
    price: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    magic_number: int = 123456
    comment: Optional[str] = None

class MT5GatewayResponse(BaseModel):
    status: str  # SUCCESS, FAILED
    ticket: Optional[int] = None
    comment: Optional[str] = None
    error_code: Optional[int] = None