import os, asyncio, logging
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Tuple, Optional
from urllib.parse import quote_plus

import aiohttp
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException

# ====== Config ======
TAKER_FEE = Decimal(os.getenv("TAKER_FEE", "0.001"))
NOTIONAL_MARGIN = Decimal(os.getenv("NOTIONAL_MARGIN", "1.02"))
BALANCE_BUFFER = Decimal(os.getenv("BALANCE_BUFFER", "0.98"))
DECIMAL_PRECISION = int(os.getenv("DECIMAL_PRECISION", "8"))
HOP_PROFIT_PCT = Decimal(os.getenv("HOP_PROFIT_PCT", "0.002"))   # 1% par hop
NUMERAIRE = os.getenv("NUMERAIRE", "SELF")                      # "USDT" ou "SELF"
START_ASSET = os.getenv("START_ASSET", "EUR")                   # actif détenu au départ

# ====== Utils ======
def fmt_decimal(d: Decimal, precision: int = DECIMAL_PRECISION) -> str:
    return format(d.quantize(Decimal(10) ** -precision), "f")

def round_step(qty: Decimal, step: Decimal) -> Decimal:
    if step==0: return qty
    return (qty/step).to_integral_value(rounding=ROUND_DOWN)*step

def extract_filters(info, symbol: str):
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            f = {f["filterType"]: f for f in s["filters"]}
            step = Decimal(f["LOT_SIZE"]["stepSize"]) if "LOT_SIZE" in f else Decimal("0.00000001")
            min_qty = Decimal(f["LOT_SIZE"]["minQty"]) if "LOT_SIZE" in f else Decimal("0")
            if "MIN_NOTIONAL" in f:
                min_notional = Decimal(f["MIN_NOTIONAL"]["minNotional"])
            elif "NOTIONAL" in f:
                min_notional = Decimal(f["NOTIONAL"]["minNotional"])
            else:
                min_notional = Decimal("0")
            return step, min_qty, min_notional
    return Decimal("0.00000001"), Decimal("0"), Decimal("0")

def get_assets(info, symbol: str):
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            return s["baseAsset"], s["quoteAsset"]
    return "BASE","QUOTE"

def best_bid(book: Dict[str, Dict[str, Decimal]], symbol: str) -> Optional[Decimal]:
    d=book.get(symbol); return d["bid"] if d else None
def best_ask(book: Dict[str, Dict[str, Decimal]], symbol: str) -> Optional[Decimal]:
    d=book.get(symbol); return d["ask"] if d else None

def get_symbol(symbol_by_assets: Dict[Tuple[str,str], str], base: str, quote: str)->Optional[str]:
    return symbol_by_assets.get((base, quote))

def get_cross_bid(book, symbol_by_assets, base: str, quote: str)->Optional[Decimal]:
    s1=get_symbol(symbol_by_assets, base, quote)
    if s1:
        b=best_bid(book, s1)
        if b: return b
    s2=get_symbol(symbol_by_assets, quote, base)
    if s2:
        a=best_ask(book, s2)
        if a and a!=0: return Decimal("1")/a
    return None

def value_in_numeraire(asset: str, amount: Decimal, book, symbol_by_assets, numeraire: str)->Decimal:
    if asset==numeraire: return amount
    px=get_cross_bid(book, symbol_by_assets, asset, numeraire)
    return amount*px if px else Decimal("0")

class ChainBot:
    def __init__(self):
        self.api_key=os.getenv("BINANCE_API_KEY")
        self.api_secret=os.getenv("BINANCE_SECRET_KEY")
        self.telegram_token=os.getenv("TELEGRAM_TOKEN")
        self.telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID")
        self.dry_run=os.getenv("DRY_RUN","1")=="1"

        self.client: Optional[AsyncClient]=None
        self.bsm: Optional[BinanceSocketManager]=None
        self.exchange_info=None

        self.book: Dict[str, Dict[str, Decimal]]={}
        self.symbol_by_assets: Dict[Tuple[str,str], str]={}
        self.by_quote: Dict[str, List[str]]={}

        self.current_asset=START_ASSET
        self.balance_eur=Decimal("0")
        self.in_trade=asyncio.Lock()

        logging.basicConfig(level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            filename="chain_bot.log")

    async def connect(self):
        self.client=await AsyncClient.create(self.api_key, self.api_secret)
        self.exchange_info=await self.client.get_exchange_info()
        self.bsm=BinanceSocketManager(self.client)
        await self._index_pairs()
        bal=await self.client.get_asset_balance(asset="EUR")
        self.balance_eur=Decimal(bal["free"])
        logging.info("Chain: start_asset=%s  HOP_PROFIT_PCT=%s  NUMERAIRE=%s", self.current_asset, HOP_PROFIT_PCT, NUMERAIRE)

    async def _index_pairs(self):
        pairs=[]
        for s in self.exchange_info["symbols"]:
            if s["status"]!="TRADING": continue
            base,quote, sym=s["baseAsset"], s["quoteAsset"], s["symbol"]
            pairs.append((base,quote,sym))
        self.symbol_by_assets.clear(); self.by_quote.clear()
        for base,quote,sym in pairs:
            self.symbol_by_assets[(base,quote)]=sym
            self.by_quote.setdefault(quote,[]).append(sym)

    async def start(self):
        assert self.bsm
        syms=set(self.symbol_by_assets.values())  # large coverage
        streams=[f"{s.lower()}@bookTicker" for s in syms]
        if not streams: logging.error("No streams"); return
        while True:
            try:
                socket=self.bsm.multiplex_socket(streams)
                async with socket as stream:
                    while True:
                        msg=await stream.recv()
                        await self.on_msg(msg)
                        await asyncio.sleep(0)
            except Exception as e:
                logging.error("WS err: %s", e); await asyncio.sleep(2)

    async def on_msg(self, msg):
        d=msg.get("data",{})
        s,b,a=d.get("s"), d.get("b"), d.get("a")
        if s and b and a:
            try: self.book[s]={"bid":Decimal(b),"ask":Decimal(a)}
            except: return
        if self.in_trade.locked(): return
        await self.check_hop()

    async def _get_free(self, asset:str)->Decimal:
        bal=await self.client.get_asset_balance(asset=asset)
        return Decimal(bal["free"])

    async def _safe_order(self, **kw):
        for i in range(3):
            try: return await self.client.create_order(**kw)
            except BinanceAPIException as e:
                logging.error("Order err (%s): code=%s msg=%s", i+1, e.code, getattr(e,"message",str(e)))
                await asyncio.sleep(0.4*(2**i))
            except Exception as e:
                logging.error("Order exc (%s): %s", i+1, e); await asyncio.sleep(0.4*(2**i))
        raise RuntimeError("order failed")

    async def check_hop(self):
        X=self.current_asset
        amount_x=self.balance_eur if X=="EUR" else await self._get_free(X)
        if amount_x<=0: return

        best=None  # (profit_pct, sym_YX, Y)
        for sym in self.by_quote.get(X, []):  # acheter base Y en payant X
            if sym not in self.book: continue
            Y,_=get_assets(self.exchange_info, sym)
            ask=self.book[sym]["ask"]; 
            if not ask: continue

            spend_x=amount_x*BALANCE_BUFFER
            amount_y=(spend_x/ask)*(1-TAKER_FEE)

            if NUMERAIRE.upper()=="SELF":
                # X->Y->X théorique
                # bid prix pour vendre Y contre X
                s1=self.symbol_by_assets.get((Y,X))
                if s1 and s1 in self.book:
                    px_bid=self.book[s1]["bid"]
                else:
                    # fallback cross via inverse
                    s2=self.symbol_by_assets.get((X,Y))
                    if s2 and s2 in self.book and self.book[s2]["ask"]!=0:
                        px_bid=Decimal("1")/self.book[s2]["ask"]
                    else:
                        continue
                x_after=amount_y*px_bid
                profit_pct=(x_after-amount_x)/amount_x
            else:
                # numéraire (USDT par défaut)
                v_now=value_in_numeraire(X, amount_x, self.book, self.symbol_by_assets, NUMERAIRE)
                v_next=value_in_numeraire(Y, amount_y, self.book, self.symbol_by_assets, NUMERAIRE)
                if v_now<=0 or v_next<=0: continue
                profit_pct=(v_next-v_now)/v_now

            if profit_pct>=HOP_PROFIT_PCT:
                # minNotional check sur BUY Y/X
                _,_,min_not=extract_filters(self.exchange_info, sym)
                if spend_x < (min_not*NOTIONAL_MARGIN): 
                    continue
                best = max(best, (profit_pct, sym, Y), key=lambda t: t[0]) if best else (profit_pct, sym, Y)

        if best:
            _, sym_YX, new_asset = best
            await self.execute_hop(sym_YX, new_asset)

    async def execute_hop(self, sym_YX: str, new_asset: str):
        async with self.in_trade:
            X=self.current_asset
            amount_x=self.balance_eur if X=="EUR" else await self._get_free(X)
            spend_x=amount_x*BALANCE_BUFFER
            _,_,min_not=extract_filters(self.exchange_info, sym_YX)
            if spend_x < (min_not*NOTIONAL_MARGIN):
                logging.info("Hop skip %s: %s < minNotional %s", sym_YX, spend_x, min_not)
                return
            if not self.dry_run:
                try:
                    await self._safe_order(symbol=sym_YX, side="BUY", type="MARKET",
                                           quoteOrderQty=fmt_decimal(spend_x))
                except Exception as e:
                    logging.error("Hop failed %s: %s", sym_YX, e); return
            self.current_asset=new_asset
            # (optionnel) rafraîchir la tréso EUR pour suivi général
            try:
                bal_eur=await self.client.get_asset_balance(asset="EUR")
                self.balance_eur=Decimal(bal_eur["free"])
            except: pass
            logging.info("Hop: %s -> %s via %s", X, new_asset, sym_YX)

    async def run(self):
        await self.connect()
        # WS sur tous les symbols indexés (couverture large)
        assert self.bsm
        streams=[f"{s.lower()}@bookTicker" for s in self.symbol_by_assets.values()]
        if not streams: logging.error("No streams"); return
        while True:
            try:
                socket=self.bsm.multiplex_socket(streams)
                async with socket as stream:
                    while True:
                        msg=await stream.recv()
                        await self.on_msg(msg)
                        await asyncio.sleep(0)
            except Exception as e:
                logging.error("WS err: %s", e); await asyncio.sleep(2)

if __name__=="__main__":
    bot=ChainBot()
    try: asyncio.run(bot.run())
    except KeyboardInterrupt: pass
