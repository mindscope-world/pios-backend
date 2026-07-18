//+------------------------------------------------------------------+
//|                                                PiOSBridgeEA.mq5  |
//|  Pi OS <-> MetaTrader 5 bridge Expert Advisor                    |
//|                                                                  |
//|  Connects OUT from the MT5 terminal to the Pi OS backend bridge  |
//|  WebSocket (/api/v1/ws/mt5/{broker_id}), authenticates with the  |
//|  broker connection's passphrase, then executes PLACE_ORDER /     |
//|  CANCEL_ORDER requests and answers PING / GET_ACCOUNT /          |
//|  GET_POSITIONS.                                                  |
//|                                                                  |
//|  SETUP (see mt5-ea/README.md for the full walkthrough):          |
//|   1. MT5: Tools -> Options -> Expert Advisors ->                 |
//|      "Allow WebRequest for listed URL" -> add the server host    |
//|      (required for SocketConnect too, not just WebRequest).      |
//|   2. Enable Algo Trading (toolbar button).                       |
//|   3. Attach this EA to any chart, fill in the inputs             |
//|      (host / port / TLS / broker id / passphrase).               |
//|   4. Pi OS UI: broker detail -> Test connection -> "EA connected"|
//|                                                                  |
//|  Wire protocol (JSON text frames over WebSocket):                |
//|   EA -> server  {"type":"HANDSHAKE","token":<passphrase>}        |
//|   server -> EA  {"type":"HANDSHAKE_ACK"}                         |
//|   server -> EA  PING / GET_ACCOUNT / GET_POSITIONS /             |
//|                 GET_ORDER / PLACE_ORDER / CANCEL_ORDER           |
//|                 (correlation_id)                                 |
//|   EA -> server  PONG / ACCOUNT_INFO / POSITIONS / ORDER_STATUS / |
//|                 ORDER_RESULT / CANCEL_RESULT (same corr id)      |
//|   EA -> server  ORDER_UPDATE — unsolicited push from             |
//|                 OnTradeTransaction when a pending order fills    |
//|                 (real deal print inline) or dies broker-side     |
//|                 (cancelled / expired / rejected)                 |
//|                                                                  |
//|  NOTE ON VOLUME: the app's order qty is sent as MT5 LOTS,        |
//|  clamped to the symbol's min/step/max.                           |
//+------------------------------------------------------------------+
#property copyright "Pi OS"
#property version   "1.10"

#include <Trade\Trade.mqh>

//--- inputs ---------------------------------------------------------
input string InpServerHost      = "localhost";  // Server host (no scheme, no path)
input int    InpServerPort      = 9000;         // Server port (443 for a public tunnel)
input bool   InpUseTLS          = false;        // Use TLS (true for wss:// / port 443)
input string InpBrokerId        = "";           // Pi OS broker connection UUID
input string InpPassphrase      = "";           // Bridge passphrase (set on the broker in Pi OS)
input string InpSymbolSuffix    = "";           // Broker symbol suffix, e.g. ".a" or ".raw"
input bool   InpMapUSDTtoUSD    = true;         // Map ...USDT/...USDC app symbols to ...USD
input int    InpDeviationPoints = 20;           // Max market-order slippage (points)
input long   InpDefaultMagic    = 987001;       // Magic number when the app sends none
input int    InpReconnectSecs   = 5;            // Reconnect delay after a drop (seconds)
input int    InpWsPingSecs      = 30;           // WebSocket keep-alive ping interval (seconds)

//--- connection state ----------------------------------------------
#define WS_CLOSED    0
#define WS_WAIT_HTTP 1
#define WS_OPEN      2

int      g_socket        = INVALID_HANDLE;
int      g_state         = WS_CLOSED;
bool     g_tls           = false;
bool     g_paired        = false;   // HANDSHAKE_ACK received
datetime g_nextConnectAt = 0;
datetime g_handshakeSent = 0;
datetime g_lastPingAt    = 0;
datetime g_lastRecvAt    = 0;   // watchdog: last time any bytes arrived
long     g_ordersDone    = 0;
long     g_ordersFailed  = 0;

uchar    g_rx[];      // raw socket receive buffer
uchar    g_frag[];    // fragmented-message accumulator
int      g_fragOp     = 0;

CTrade   trade;

//+------------------------------------------------------------------+
//| Lifecycle                                                        |
//+------------------------------------------------------------------+
int OnInit()
  {
   if(InpBrokerId == "" || InpPassphrase == "")
     {
      Print("PiOS bridge: InpBrokerId and InpPassphrase are required inputs.");
      return(INIT_PARAMETERS_INCORRECT);
     }
   MathSrand((int)GetTickCount());
   trade.SetAsyncMode(false);
   trade.SetDeviationInPoints(InpDeviationPoints);
   EventSetMillisecondTimer(100);
   UpdateComment("starting");
   return(INIT_SUCCEEDED);
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
   CloseSocket("EA removed");
   Comment("");
  }

void OnTimer()
  {
   if(g_state == WS_CLOSED)
     {
      if(TimeCurrent() >= g_nextConnectAt)
         Connect();
      return;
     }

   if(!SocketIsConnected(g_socket))
     {
      CloseSocket("socket dropped");
      return;
     }

   PollSocket();

   // pairing timeout: server silently closing = bad token / unknown broker id
   if(g_state == WS_OPEN && !g_paired && g_handshakeSent > 0 &&
      TimeCurrent() - g_handshakeSent > 10)
     {
      Print("PiOS bridge: no HANDSHAKE_ACK after 10s -- check the broker id and passphrase ",
            "(the server closes the socket on a bad token).");
      CloseSocket("handshake timeout");
      return;
     }

   // WebSocket-level keep-alive (also keeps tunnels/NAT from idling out)
   if(g_state == WS_OPEN && InpWsPingSecs > 0 &&
      TimeCurrent() - g_lastPingAt >= InpWsPingSecs)
     {
      uchar none[];
      WsSendFrame(0x9, none, 0);
      g_lastPingAt = TimeCurrent();
     }

   // Dead-link watchdog: our pings elicit protocol-level pongs, so a healthy
   // link never goes quiet for long. A half-open socket (backend restart,
   // tunnel drop behind NAT) keeps SocketIsConnected() true indefinitely --
   // silence is the only reliable death signal.
   if(InpWsPingSecs > 0 && g_lastRecvAt > 0 &&
      TimeCurrent() - g_lastRecvAt > 3 * InpWsPingSecs)
     {
      CloseSocket("server unresponsive for " + IntegerToString(3 * InpWsPingSecs) + "s");
      return;
     }
  }

//+------------------------------------------------------------------+
//| Connection management                                            |
//+------------------------------------------------------------------+
void Connect()
  {
   g_nextConnectAt = TimeCurrent() + InpReconnectSecs;

   g_socket = SocketCreate();
   if(g_socket == INVALID_HANDLE)
     {
      Print("PiOS bridge: SocketCreate failed, error ", GetLastError());
      return;
     }

   if(!SocketConnect(g_socket, InpServerHost, InpServerPort, 5000))
     {
      int err = GetLastError();
      Print("PiOS bridge: cannot connect to ", InpServerHost, ":", InpServerPort,
            " (error ", err, "). Is the host in Tools -> Options -> Expert Advisors -> ",
            "'Allow WebRequest for listed URL'?");
      SocketClose(g_socket);
      g_socket = INVALID_HANDLE;
      UpdateComment("connect failed");
      return;
     }

   // Port 443 is TLS automatically; any other TLS port needs an explicit handshake.
   g_tls = (InpUseTLS || InpServerPort == 443);
   if(g_tls && InpServerPort != 443)
     {
      if(!SocketTlsHandshake(g_socket, InpServerHost))
        {
         Print("PiOS bridge: TLS handshake failed, error ", GetLastError());
         CloseSocket("tls failed");
         return;
        }
     }

   // HTTP -> WebSocket upgrade
   uchar keyBytes[16];
   for(int i = 0; i < 16; i++)
      keyBytes[i] = (uchar)(MathRand() % 256);
   uchar b64[];
   uchar nokey[];
   CryptEncode(CRYPT_BASE64, keyBytes, nokey, b64);
   string wsKey = CharArrayToString(b64, 0, WHOLE_ARRAY, CP_UTF8);

   string path = "/api/v1/ws/mt5/" + InpBrokerId;
   string req  = "GET " + path + " HTTP/1.1\r\n" +
                 "Host: " + InpServerHost + "\r\n" +
                 "Upgrade: websocket\r\n" +
                 "Connection: Upgrade\r\n" +
                 "Sec-WebSocket-Key: " + wsKey + "\r\n" +
                 "Sec-WebSocket-Version: 13\r\n" +
                 "\r\n";

   ArrayResize(g_rx, 0);
   ArrayResize(g_frag, 0);
   g_paired        = false;
   g_handshakeSent = 0;
   g_lastRecvAt    = TimeCurrent();
   g_state         = WS_WAIT_HTTP;

   if(!SendString(req))
     {
      CloseSocket("upgrade send failed");
      return;
     }
   Print("PiOS bridge: connecting to ", (g_tls ? "wss://" : "ws://"),
         InpServerHost, ":", InpServerPort, path);
   UpdateComment("upgrading");
  }

void CloseSocket(const string why)
  {
   if(g_socket != INVALID_HANDLE)
     {
      SocketClose(g_socket);
      g_socket = INVALID_HANDLE;
     }
   if(g_state != WS_CLOSED || g_paired)
      Print("PiOS bridge: disconnected (", why, "), retrying in ", InpReconnectSecs, "s");
   g_state         = WS_CLOSED;
   g_paired        = false;
   g_nextConnectAt = TimeCurrent() + InpReconnectSecs;
   ArrayResize(g_rx, 0);
   ArrayResize(g_frag, 0);
   UpdateComment("disconnected");
  }

//+------------------------------------------------------------------+
//| Socket IO (plain or TLS)                                         |
//+------------------------------------------------------------------+
bool SendRaw(uchar &data[], const int len)
  {
   int sent = 0;
   while(sent < len)
     {
      uchar part[];
      ArrayResize(part, len - sent);
      ArrayCopy(part, data, 0, sent, len - sent);
      int n = g_tls ? SocketTlsSend(g_socket, part, (uint)(len - sent))
                    : SocketSend(g_socket, part, (uint)(len - sent));
      if(n <= 0)
        {
         Print("PiOS bridge: send failed, error ", GetLastError());
         return(false);
        }
      sent += n;
     }
   return(true);
  }

bool SendString(const string s)
  {
   uchar data[];
   int n = StringToCharArray(s, data, 0, WHOLE_ARRAY, CP_UTF8) - 1; // drop trailing \0
   if(n <= 0)
      return(false);
   ArrayResize(data, n);
   return(SendRaw(data, n));
  }

// Pull whatever is available off the socket into g_rx, then parse.
void PollSocket()
  {
   for(int guard = 0; guard < 16; guard++)
     {
      uchar chunk[];
      int   got = 0;
      if(g_tls)
        {
         got = SocketTlsReadAvailable(g_socket, chunk, 65536);
        }
      else
        {
         uint avail = SocketIsReadable(g_socket);
         if(avail == 0)
            break;
         got = SocketRead(g_socket, chunk, avail, 50);
        }
      if(got < 0)
        {
         CloseSocket("read error");
         return;
        }
      if(got == 0)
         break;
      g_lastRecvAt = TimeCurrent();
      int base = ArraySize(g_rx);
      ArrayResize(g_rx, base + got);
      ArrayCopy(g_rx, chunk, base, 0, got);
     }
   ProcessBuffer();
  }

void ConsumeRx(const int n)
  {
   int len = ArraySize(g_rx);
   if(n >= len)
     {
      ArrayResize(g_rx, 0);
      return;
     }
   uchar rest[];
   ArrayResize(rest, len - n);
   ArrayCopy(rest, g_rx, 0, n, len - n);
   ArrayFree(g_rx);
   ArrayCopy(g_rx, rest);
  }

//+------------------------------------------------------------------+
//| WebSocket framing                                                |
//+------------------------------------------------------------------+
bool WsSendFrame(const int opcode, uchar &payload[], const int plen)
  {
   int hdr = 2 + 4;                       // base header + mask key
   if(plen > 65535)      hdr += 8;
   else if(plen > 125)   hdr += 2;

   uchar frame[];
   ArrayResize(frame, hdr + plen);
   frame[0] = (uchar)(0x80 | opcode);     // FIN + opcode
   int p = 2;
   if(plen > 65535)
     {
      frame[1] = 0x80 | 127;
      long l = plen;
      for(int i = 7; i >= 0; i--)
        {
         frame[2 + i] = (uchar)(l & 0xFF);
         l >>= 8;
        }
      p = 10;
     }
   else if(plen > 125)
     {
      frame[1] = 0x80 | 126;
      frame[2] = (uchar)((plen >> 8) & 0xFF);
      frame[3] = (uchar)(plen & 0xFF);
      p = 4;
     }
   else
      frame[1] = (uchar)(0x80 | plen);

   uchar mask[4];
   for(int i = 0; i < 4; i++)
     {
      mask[i]      = (uchar)(MathRand() % 256);
      frame[p + i] = mask[i];
     }
   p += 4;
   for(int i = 0; i < plen; i++)
      frame[p + i] = (uchar)(payload[i] ^ mask[i % 4]);

   return(SendRaw(frame, hdr + plen));
  }

bool WsSendText(const string s)
  {
   uchar data[];
   int n = StringToCharArray(s, data, 0, WHOLE_ARRAY, CP_UTF8) - 1;
   if(n < 0)
      return(false);
   ArrayResize(data, MathMax(n, 0));
   return(WsSendFrame(0x1, data, n));
  }

void ProcessBuffer()
  {
   while(true)
     {
      int len = ArraySize(g_rx);

      if(g_state == WS_WAIT_HTTP)
        {
         int end = -1;
         for(int i = 0; i + 3 < len; i++)
            if(g_rx[i] == 13 && g_rx[i+1] == 10 && g_rx[i+2] == 13 && g_rx[i+3] == 10)
              {
               end = i + 4;
               break;
              }
         if(end < 0)
            return;
         string hdr = CharArrayToString(g_rx, 0, end, CP_UTF8);
         ConsumeRx(end);
         if(StringFind(hdr, " 101 ") < 0)
           {
            Print("PiOS bridge: WebSocket upgrade rejected:\n", hdr);
            CloseSocket("upgrade rejected");
            return;
           }
         g_state = WS_OPEN;
         Print("PiOS bridge: WebSocket open, sending HANDSHAKE");
         WsSendText("{\"type\":\"HANDSHAKE\",\"token\":\"" + JsonEscape(InpPassphrase) + "\"}");
         g_handshakeSent = TimeCurrent();
         g_lastPingAt    = TimeCurrent();
         UpdateComment("pairing");
         continue;
        }

      if(g_state != WS_OPEN || len < 2)
         return;

      uchar b0     = g_rx[0];
      uchar b1     = g_rx[1];
      int   opcode = b0 & 0x0F;
      bool  fin    = (b0 & 0x80) != 0;
      bool  masked = (b1 & 0x80) != 0;
      long  plen   = b1 & 0x7F;
      int   hoff   = 2;

      if(plen == 126)
        {
         if(len < 4) return;
         plen = ((long)g_rx[2] << 8) | g_rx[3];
         hoff = 4;
        }
      else if(plen == 127)
        {
         if(len < 10) return;
         plen = 0;
         for(int i = 2; i < 10; i++)
            plen = (plen << 8) | g_rx[i];
         hoff = 10;
        }
      int maskOff = hoff;
      if(masked)
         hoff += 4;
      if(len < hoff + plen)
         return;

      uchar payload[];
      ArrayResize(payload, (int)plen);
      for(int i = 0; i < plen; i++)
        {
         uchar c = g_rx[hoff + i];
         if(masked)
            c ^= g_rx[maskOff + (i % 4)];
         payload[i] = c;
        }
      ConsumeRx(hoff + (int)plen);
      HandleFrame(opcode, fin, payload);
      if(g_state == WS_CLOSED)
         return;
     }
  }

void HandleFrame(const int opcode, const bool fin, uchar &payload[])
  {
   int plen = ArraySize(payload);

   if(opcode == 0x8)                      // close
     {
      CloseSocket("server sent close");
      return;
     }
   if(opcode == 0x9)                      // ping -> pong (echo payload)
     {
      WsSendFrame(0xA, payload, plen);
      return;
     }
   if(opcode == 0xA)                      // pong
      return;

   if(opcode == 0x1 || opcode == 0x2 || opcode == 0x0)
     {
      // accumulate fragments (rare for this protocol, but be correct)
      if(opcode != 0x0)
        {
         ArrayResize(g_frag, 0);
         g_fragOp = opcode;
        }
      int base = ArraySize(g_frag);
      ArrayResize(g_frag, base + plen);
      if(plen > 0)
         ArrayCopy(g_frag, payload, base, 0, plen);
      if(!fin)
         return;
      string msg = CharArrayToString(g_frag, 0, WHOLE_ARRAY, CP_UTF8);
      ArrayResize(g_frag, 0);
      OnJsonMessage(msg);
     }
  }

//+------------------------------------------------------------------+
//| Bridge protocol                                                  |
//+------------------------------------------------------------------+
void OnJsonMessage(const string msg)
  {
   string type = JsonGetString(msg, "type");
   string corr = JsonGetString(msg, "correlation_id");

   if(type == "HANDSHAKE_ACK")
     {
      g_paired = true;
      Print("PiOS bridge: paired -- EA connected for broker ", InpBrokerId);
      UpdateComment("CONNECTED");
      return;
     }
   if(type == "PING")
     {
      WsSendText("{\"type\":\"PONG\",\"correlation_id\":\"" + JsonEscape(corr) + "\"}");
      return;
     }
   if(type == "GET_ACCOUNT")
     {
      SendAccountInfo(corr);
      return;
     }
   if(type == "GET_POSITIONS")
     {
      SendPositions(corr);
      return;
     }
   if(type == "GET_ORDER")
     {
      HandleGetOrder(msg, corr);
      return;
     }
   if(type == "PLACE_ORDER")
     {
      HandlePlaceOrder(msg, corr);
      return;
     }
   if(type == "CANCEL_ORDER")
     {
      HandleCancelOrder(msg, corr);
      return;
     }
   Print("PiOS bridge: unknown message type '", type, "' -- ignored");
  }

void SendAccountInfo(const string corr)
  {
   string reply = "{\"type\":\"ACCOUNT_INFO\"" +
      ",\"correlation_id\":\"" + JsonEscape(corr) + "\"" +
      ",\"buying_power\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) +
      ",\"equity\":"       + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) +
      ",\"balance\":"      + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) +
      ",\"currency\":\""   + JsonEscape(AccountInfoString(ACCOUNT_CURRENCY)) + "\"" +
      ",\"login\":"        + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) +
      ",\"server\":\""     + JsonEscape(AccountInfoString(ACCOUNT_SERVER)) + "\"" +
      ",\"leverage\":"     + IntegerToString(AccountInfoInteger(ACCOUNT_LEVERAGE)) +
      "}";
   WsSendText(reply);
  }

void SendPositions(const string corr)
  {
   string arr = "";
   int total = PositionsTotal();
   for(int i = 0; i < total; i++)
     {
      string psym = PositionGetSymbol(i);      // also selects the position
      if(psym == "")
         continue;
      double vol    = PositionGetDouble(POSITION_VOLUME);
      double signed_qty = (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_SELL) ? -vol : vol;
      double open   = PositionGetDouble(POSITION_PRICE_OPEN);
      double profit = PositionGetDouble(POSITION_PROFIT);

      string norm = psym;
      int sfx = StringLen(InpSymbolSuffix);
      if(sfx > 0 && StringLen(psym) > sfx &&
         StringSubstr(psym, StringLen(psym) - sfx) == InpSymbolSuffix)
         norm = StringSubstr(psym, 0, StringLen(psym) - sfx);

      if(arr != "")
         arr += ",";
      arr += "{\"symbol\":\"" + JsonEscape(norm) + "\"" +
             ",\"qty\":"             + DoubleToString(signed_qty, 8) +
             ",\"avg_entry_price\":" + DoubleToString(open, 8) +
             ",\"unrealized_pl\":"   + DoubleToString(profit, 2) + "}";
     }
   WsSendText("{\"type\":\"POSITIONS\",\"correlation_id\":\"" + JsonEscape(corr) +
              "\",\"positions\":[" + arr + "]}");
  }

void HandlePlaceOrder(const string msg, const string corr)
  {
   string appSymbol = JsonGetString(msg, "symbol");
   string action    = JsonGetString(msg, "action");      // BUY | SELL
   string orderType = JsonGetString(msg, "order_type");  // MARKET | LIMIT | STOP | ...
   double volume    = JsonGetDouble(msg, "volume");
   double price     = JsonGetDouble(msg, "price");
   double stopPrice = JsonGetDouble(msg, "stop_price");
   long   magic     = (long)JsonGetDouble(msg, "magic");
   string comment   = JsonGetString(msg, "comment");
   if(StringLen(comment) > 25)
      comment = StringSubstr(comment, 0, 25);

   string sym = ResolveSymbol(appSymbol);
   if(sym == "")
     {
      SendOrderError(corr, "SYMBOL_NOT_FOUND",
                     "No MT5 symbol matches '" + appSymbol + "' (suffix input: '" +
                     InpSymbolSuffix + "')");
      return;
     }

   double vol = NormalizeVolume(sym, volume);
   if(vol <= 0)
     {
      SendOrderError(corr, "BAD_VOLUME",
                     "Volume " + DoubleToString(volume, 8) + " below minimum for " + sym);
      return;
     }

   int digits = (int)SymbolInfoInteger(sym, SYMBOL_DIGITS);
   trade.SetExpertMagicNumber(magic > 0 ? magic : InpDefaultMagic);
   trade.SetTypeFillingBySymbol(sym);

   bool isBuy = (action == "BUY");
   bool sent  = false;

   if(orderType == "MARKET")
     {
      sent = isBuy ? trade.Buy(vol, sym, 0.0, 0.0, 0.0, comment)
                   : trade.Sell(vol, sym, 0.0, 0.0, 0.0, comment);
     }
   else if(orderType == "LIMIT")
     {
      if(price <= 0)
        {
         SendOrderError(corr, "BAD_PRICE", "LIMIT order without a price");
         return;
        }
      double p = NormalizeDouble(price, digits);
      sent = isBuy ? trade.BuyLimit(vol, p, sym, 0.0, 0.0, ORDER_TIME_GTC, 0, comment)
                   : trade.SellLimit(vol, p, sym, 0.0, 0.0, ORDER_TIME_GTC, 0, comment);
     }
   else if(orderType == "STOP")
     {
      double trigger = (stopPrice > 0 ? stopPrice : price);
      if(trigger <= 0)
        {
         SendOrderError(corr, "BAD_PRICE", "STOP order without a trigger price");
         return;
        }
      double p = NormalizeDouble(trigger, digits);
      sent = isBuy ? trade.BuyStop(vol, p, sym, 0.0, 0.0, ORDER_TIME_GTC, 0, comment)
                   : trade.SellStop(vol, p, sym, 0.0, 0.0, ORDER_TIME_GTC, 0, comment);
     }
   else
     {
      // TWAP/VWAP/ICEBERG/STOP_LIMIT/OCO are handled app-side; the EA only
      // ever sees their child MARKET/LIMIT slices.
      SendOrderError(corr, "UNSUPPORTED_TYPE",
                     "Order type '" + orderType + "' is not executable EA-side");
      return;
     }

   uint rc = trade.ResultRetcode();
   bool ok = sent && (rc == TRADE_RETCODE_DONE ||
                      rc == TRADE_RETCODE_DONE_PARTIAL ||
                      rc == TRADE_RETCODE_PLACED);
   if(!ok)
     {
      g_ordersFailed++;
      SendOrderError(corr, IntegerToString(rc),
                     "MT5 rejected: " + trade.ResultRetcodeDescription());
      UpdateComment("CONNECTED");
      return;
     }

   ulong ticket = trade.ResultOrder();
   if(ticket == 0)
      ticket = trade.ResultDeal();

   string reply = "{\"type\":\"ORDER_RESULT\"" +
      ",\"correlation_id\":\"" + JsonEscape(corr) + "\"" +
      ",\"success\":true" +
      ",\"ticket\":\"" + IntegerToString((long)ticket) + "\"";

   // market executions report the fill inline; pending orders stay SUBMITTED
   if(orderType == "MARKET" && trade.ResultVolume() > 0)
     {
      reply += ",\"avg_fill_price\":" + DoubleToString(trade.ResultPrice(), digits) +
               ",\"filled_qty\":"     + DoubleToString(trade.ResultVolume(), 8);
     }
   reply += "}";
   WsSendText(reply);

   g_ordersDone++;
   Print("PiOS bridge: ", orderType, " ", action, " ", DoubleToString(vol, 2), " ", sym,
         " -> ticket ", (long)ticket, " (", trade.ResultRetcodeDescription(), ")");
   UpdateComment("CONNECTED");
  }

void SendOrderError(const string corr, const string code, const string message)
  {
   g_ordersFailed++;
   Print("PiOS bridge: order rejected -- ", message);
   WsSendText("{\"type\":\"ORDER_RESULT\"" +
              ",\"correlation_id\":\"" + JsonEscape(corr) + "\"" +
              ",\"success\":false" +
              ",\"error_code\":\""    + JsonEscape(code) + "\"" +
              ",\"error_message\":\"" + JsonEscape(message) + "\"}");
  }

void HandleCancelOrder(const string msg, const string corr)
  {
   string idStr  = JsonGetString(msg, "broker_order_id");
   ulong  ticket = (ulong)StringToInteger(idStr);
   bool   ok     = false;
   string err    = "";

   if(ticket == 0)
      err = "Bad ticket '" + idStr + "'";
   else if(!OrderSelect(ticket))
      err = "No pending order with ticket " + idStr + " (already filled or cancelled?)";
   else
     {
      ok = trade.OrderDelete(ticket);
      if(!ok)
         err = trade.ResultRetcodeDescription();
     }

   string reply = "{\"type\":\"CANCEL_RESULT\"" +
                  ",\"correlation_id\":\"" + JsonEscape(corr) + "\"" +
                  ",\"success\":" + (ok ? "true" : "false");
   if(!ok)
      reply += ",\"error_message\":\"" + JsonEscape(err) + "\"";
   reply += "}";
   WsSendText(reply);
   Print("PiOS bridge: cancel ", idStr, " -> ", ok ? "OK" : err);
  }

//+------------------------------------------------------------------+
//| Order state reporting (fill sync)                                |
//|                                                                  |
//| The server keeps resting LIMIT/STOP orders in sync two ways:     |
//|  - push: OnTradeTransaction sends ORDER_UPDATE the moment a deal |
//|    executes against one of our tickets (real print price inline) |
//|    or a pending order is cancelled/expired/rejected broker-side. |
//|  - poll: the server asks GET_ORDER for each open ticket and gets |
//|    ORDER_STATUS back — the safety net for updates that landed    |
//|    while the EA was disconnected.                                |
//| Status vocabulary matches the app: SUBMITTED / PARTIAL / FILLED /|
//| CANCELLED / EXPIRED / REJECTED, plus UNKNOWN when the ticket is  |
//| in neither the working set nor history (wrong account?).         |
//+------------------------------------------------------------------+

// Cumulative executed volume + volume-weighted average price of every
// history deal generated by order `ticket`. `setup` bounds the
// HistorySelect window (the order's setup time; pass 0 for all history).
void OrderDealTotals(const ulong ticket, const datetime setup,
                     double &filled, double &avg)
  {
   filled = 0.0;
   avg    = 0.0;
   double notional = 0.0;
   datetime from = (setup > 86400 ? setup - 86400 : 0);
   if(!HistorySelect(from, TimeCurrent() + 86400))
      return;
   int total = HistoryDealsTotal();
   for(int i = 0; i < total; i++)
     {
      ulong d = HistoryDealGetTicket(i);
      if(d == 0)
         continue;
      if((ulong)HistoryDealGetInteger(d, DEAL_ORDER) != ticket)
         continue;
      double v = HistoryDealGetDouble(d, DEAL_VOLUME);
      double p = HistoryDealGetDouble(d, DEAL_PRICE);
      filled   += v;
      notional += v * p;
     }
   if(filled > 0)
      avg = notional / filled;
  }

// Resolve a ticket's app-vocabulary status + cumulative fill state,
// whether it's still working or already in history.
string TicketState(const ulong ticket, double &filled, double &avg)
  {
   filled = 0.0;
   avg    = 0.0;
   if(ticket == 0)
      return("UNKNOWN");

   if(OrderSelect(ticket))            // still working (pending)
     {
      datetime setup = (datetime)OrderGetInteger(ORDER_TIME_SETUP);
      OrderDealTotals(ticket, setup, filled, avg);
      return(filled > 0 ? "PARTIAL" : "SUBMITTED");
     }

   if(HistoryOrderSelect(ticket))     // done -- filled or dead
     {
      // read the order's properties before OrderDealTotals reshapes the
      // history selection
      datetime setup = (datetime)HistoryOrderGetInteger(ticket, ORDER_TIME_SETUP);
      ENUM_ORDER_STATE st =
         (ENUM_ORDER_STATE)HistoryOrderGetInteger(ticket, ORDER_STATE);
      OrderDealTotals(ticket, setup, filled, avg);
      if(st == ORDER_STATE_FILLED)   return("FILLED");
      if(st == ORDER_STATE_CANCELED) return("CANCELLED");
      if(st == ORDER_STATE_EXPIRED)  return("EXPIRED");
      if(st == ORDER_STATE_REJECTED) return("REJECTED");
      if(st == ORDER_STATE_PARTIAL)  return("PARTIAL");
      return(filled > 0 ? "FILLED" : "SUBMITTED");
     }

   return("UNKNOWN");
  }

void HandleGetOrder(const string msg, const string corr)
  {
   string idStr  = JsonGetString(msg, "broker_order_id");
   ulong  ticket = (ulong)StringToInteger(idStr);
   double filled = 0.0, avg = 0.0;
   string status = TicketState(ticket, filled, avg);

   WsSendText("{\"type\":\"ORDER_STATUS\"" +
              ",\"correlation_id\":\"" + JsonEscape(corr) + "\"" +
              ",\"ticket\":\"" + JsonEscape(idStr) + "\"" +
              ",\"status\":\"" + status + "\"" +
              ",\"filled_qty\":"     + DoubleToString(filled, 8) +
              ",\"avg_fill_price\":" + DoubleToString(avg, 8) + "}");
  }

void SendOrderUpdate(const ulong ticket, const string status,
                     const double filled, const double avg,
                     const double dealVol, const double dealPrice)
  {
   string out = "{\"type\":\"ORDER_UPDATE\"" +
      ",\"ticket\":\"" + IntegerToString((long)ticket) + "\"" +
      ",\"status\":\"" + status + "\"" +
      ",\"filled_qty\":"     + DoubleToString(filled, 8) +
      ",\"avg_fill_price\":" + DoubleToString(avg, 8);
   if(dealVol > 0)
      out += ",\"fill_qty\":"   + DoubleToString(dealVol, 8) +
             ",\"fill_price\":" + DoubleToString(dealPrice, 8);
   out += "}";
   WsSendText(out);
   Print("PiOS bridge: ORDER_UPDATE ticket ", (long)ticket, " ", status,
         " (filled ", DoubleToString(filled, 2), ")");
  }

void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
  {
   if(g_state != WS_OPEN || !g_paired)
      return;

   if(trans.type == TRADE_TRANSACTION_DEAL_ADD)
     {
      // A deal executed. Report it against its originating order ticket --
      // that's what the app stored as broker_order_id. The server ignores
      // tickets it never placed, so manual trades filtered here (magic 0)
      // are belt-and-braces, not load-bearing.
      if(!HistoryDealSelect(trans.deal))
         return;
      if(HistoryDealGetInteger(trans.deal, DEAL_MAGIC) == 0)
         return;                      // manual terminal trade
      ulong orderTicket = (ulong)HistoryDealGetInteger(trans.deal, DEAL_ORDER);
      if(orderTicket == 0)
         return;
      double dealVol   = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);
      double dealPrice = HistoryDealGetDouble(trans.deal, DEAL_PRICE);

      double filled = 0.0, avg = 0.0;
      string status = TicketState(orderTicket, filled, avg);
      if(status == "UNKNOWN")         // history race -- trust the deal itself
        {
         status = "FILLED";
         filled = dealVol;
         avg    = dealPrice;
        }
      SendOrderUpdate(orderTicket, status, filled, avg, dealVol, dealPrice);
      return;
     }

   if(trans.type == TRADE_TRANSACTION_HISTORY_ADD)
     {
      // An order left the working set. Fills are reported by DEAL_ADD
      // above -- this branch mirrors broker-side cancels/expiries/rejects.
      if(!HistoryOrderSelect(trans.order))
         return;
      if(HistoryOrderGetInteger(trans.order, ORDER_MAGIC) == 0)
         return;
      ENUM_ORDER_STATE st =
         (ENUM_ORDER_STATE)HistoryOrderGetInteger(trans.order, ORDER_STATE);
      string status = "";
      if(st == ORDER_STATE_CANCELED)      status = "CANCELLED";
      else if(st == ORDER_STATE_EXPIRED)  status = "EXPIRED";
      else if(st == ORDER_STATE_REJECTED) status = "REJECTED";
      else
         return;
      datetime setup = (datetime)HistoryOrderGetInteger(trans.order, ORDER_TIME_SETUP);
      double filled = 0.0, avg = 0.0;
      OrderDealTotals(trans.order, setup, filled, avg);
      SendOrderUpdate(trans.order, status, filled, avg, 0.0, 0.0);
      return;
     }
  }

//+------------------------------------------------------------------+
//| Symbol / volume helpers                                          |
//+------------------------------------------------------------------+
string ResolveSymbol(const string appSymbol)
  {
   string base = appSymbol;
   StringToUpper(base);
   StringReplace(base, "/", "");
   StringReplace(base, " ", "");

   string mapped = base;
   if(InpMapUSDTtoUSD)
     {
      if(StringLen(base) > 4 && StringSubstr(base, StringLen(base) - 4) == "USDT")
         mapped = StringSubstr(base, 0, StringLen(base) - 4) + "USD";
      else if(StringLen(base) > 4 && StringSubstr(base, StringLen(base) - 4) == "USDC")
         mapped = StringSubstr(base, 0, StringLen(base) - 4) + "USD";
     }

   string candidates[6];
   int n = 0;
   candidates[n++] = appSymbol;
   candidates[n++] = base;
   if(mapped != base)
      candidates[n++] = mapped;
   if(InpSymbolSuffix != "")
     {
      candidates[n++] = base + InpSymbolSuffix;
      if(mapped != base)
         candidates[n++] = mapped + InpSymbolSuffix;
     }

   for(int i = 0; i < n; i++)
     {
      if(candidates[i] == "")
         continue;
      if(SymbolSelect(candidates[i], true))
         return(candidates[i]);
     }
   return("");
  }

double NormalizeVolume(const string sym, const double requested)
  {
   double minv = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN);
   double maxv = SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
   if(requested <= 0)
      return(0);
   double vol = requested;
   if(step > 0)
      vol = MathRound(vol / step) * step;
   if(vol < minv)
      vol = minv;
   if(maxv > 0 && vol > maxv)
      vol = maxv;
   return(NormalizeDouble(vol, 8));
  }

//+------------------------------------------------------------------+
//| Minimal JSON helpers (flat objects, which is all this protocol   |
//| uses server->EA)                                                 |
//+------------------------------------------------------------------+
int JsonFindValue(const string json, const string key)
  {
   string pat = "\"" + key + "\"";
   int p = StringFind(json, pat);
   if(p < 0)
      return(-1);
   p += StringLen(pat);
   int len = StringLen(json);
   bool colonSeen = false;
   while(p < len)
     {
      ushort c = StringGetCharacter(json, p);
      if(c == ':')
        {
         colonSeen = true;
         p++;
         continue;
        }
      if(c == ' ' || c == '\t' || c == '\r' || c == '\n')
        {
         p++;
         continue;
        }
      break;
     }
   if(!colonSeen || p >= len)
      return(-1);
   return(p);
  }

string JsonGetString(const string json, const string key, const string def = "")
  {
   int p = JsonFindValue(json, key);
   if(p < 0)
      return(def);
   int len = StringLen(json);
   if(StringGetCharacter(json, p) != '"')
     {
      string tok = JsonToken(json, p);
      return(tok == "null" ? def : tok);
     }
   p++;
   string out = "";
   while(p < len)
     {
      ushort c = StringGetCharacter(json, p);
      if(c == '"')
         break;
      if(c == '\\' && p + 1 < len)
        {
         p++;
         ushort e = StringGetCharacter(json, p);
         switch(e)
           {
            case '"':  out += "\"";  break;
            case '\\': out += "\\";  break;
            case '/':  out += "/";   break;
            case 'n':  out += "\n";  break;
            case 't':  out += "\t";  break;
            case 'r':  out += "\r";  break;
            case 'b':  out += ShortToString(8);  break;
            case 'f':  out += ShortToString(12); break;
            case 'u':
               if(p + 4 < len)
                 {
                  out += ShortToString(HexToUShort(StringSubstr(json, p + 1, 4)));
                  p += 4;
                 }
               break;
            default:   out += ShortToString(e);
           }
        }
      else
         out += ShortToString(c);
      p++;
     }
   return(out);
  }

string JsonToken(const string json, int p)
  {
   int len = StringLen(json);
   string tok = "";
   while(p < len)
     {
      ushort c = StringGetCharacter(json, p);
      if(c == ',' || c == '}' || c == ']' || c == ' ' ||
         c == '\t' || c == '\r' || c == '\n')
         break;
      tok += ShortToString(c);
      p++;
     }
   return(tok);
  }

double JsonGetDouble(const string json, const string key, const double def = 0.0)
  {
   int p = JsonFindValue(json, key);
   if(p < 0)
      return(def);
   ushort c = StringGetCharacter(json, p);
   string tok;
   if(c == '"')
      tok = JsonGetString(json, key);
   else
      tok = JsonToken(json, p);
   if(tok == "" || tok == "null")
      return(def);
   return(StringToDouble(tok));
  }

ushort HexToUShort(const string hex)
  {
   ushort v = 0;
   for(int i = 0; i < StringLen(hex); i++)
     {
      ushort c = StringGetCharacter(hex, i);
      int d;
      if(c >= '0' && c <= '9')      d = (int)(c - '0');
      else if(c >= 'a' && c <= 'f') d = (int)(c - 'a') + 10;
      else if(c >= 'A' && c <= 'F') d = (int)(c - 'A') + 10;
      else break;
      v = (ushort)(v * 16 + d);
     }
   return(v);
  }

string JsonEscape(const string s)
  {
   string out = s;
   StringReplace(out, "\\", "\\\\");
   StringReplace(out, "\"", "\\\"");
   StringReplace(out, "\n", "\\n");
   StringReplace(out, "\r", "\\r");
   StringReplace(out, "\t", "\\t");
   return(out);
  }

//+------------------------------------------------------------------+
//| Chart status line                                                |
//+------------------------------------------------------------------+
void UpdateComment(const string status)
  {
   Comment("PiOS bridge  |  ", status,
           "  |  ", (g_tls ? "wss://" : "ws://"), InpServerHost, ":", InpServerPort,
           "\nbroker ", InpBrokerId,
           "\norders ok: ", g_ordersDone, "   rejected: ", g_ordersFailed);
  }
//+------------------------------------------------------------------+
