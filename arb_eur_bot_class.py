import os
import asyncio
import logging
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Tuple

import aiohttp
from urllib.parse import quote_plus
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException

# =======================
# Paramètres de sécurité
# =======================
TAKER_FEE = Decimal(os.getenv("TAKER_FEE", "0.00095"))          # 0.1% par ordre (market = taker)
NOTIONAL_MARGIN = Decimal(os.getenv("NOTIONAL_MARGIN", "1.02"))  # +2% marge vs minNotional
BALANCE_BUFFER = Decimal(os.getenv("BALANCE_BUFFER", "0.97"))     # n’utiliser qu’~98% du solde réel
MIN_PROFIT_PCT = Decimal(os.getenv("MIN_PROFIT_PCT", "0.007"))    # seuil % net (~1%)
DECIMAL_PRECISION = int(os.getenv("DECIMAL_PRECISION", "8"))


# =======================
# Utilitaires
# =======================
def fmt_decimal(d: Decimal, precision: int = DECIMAL_PRECISION) -> str:
    """Formate un Decimal en string sans notation scientifique, limité en décimales."""
    return format(d.quantize(Decimal(10) ** -precision), "f")

def round_step(qty: Decimal, step: Decimal) -> Decimal:
    """Arrondit vers le bas au multiple de step."""
    if step == 0:
        return qty
    return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step

def extract_filters(info, symbol: str) -> Tuple[Decimal, Decimal, Decimal]:
    """Retourne (stepSize, minQty, minNotional) pour un symbole."""
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

def get_assets(info, symbol: str) -> Tuple[str, str]:
    """Retourne (baseAsset, quoteAsset) du symbole."""
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            return s["baseAsset"], s["quoteAsset"]
    return "BASE", "QUOTE"

def get_quote_asset(info, symbol: str) -> str:
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            return s["quoteAsset"]
    return "QUOTE"


class ArbBot:
    """Triangular arbitrage bot starting/ending in EUR on Binance, robustifié."""

    def __init__(self) -> None:
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_SECRET_KEY")  # respecte ton nom d'ENV
        self.telegram_token = os.getenv("TELEGRAM_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.dry_run = os.getenv("DRY_RUN", "1") == "1"

        self.client: AsyncClient | None = None
        self.bsm: BinanceSocketManager | None = None

        # book[symbol] = {"bid": Decimal, "ask": Decimal}
        self.book: Dict[str, Dict[str, Decimal]] = {}
        self.cycles: List[Tuple[str, str, str]] = []

        self.balance_eur: Decimal = Decimal("0")
        self.cumulative_profit: Decimal = Decimal("0")
        self.in_trade: asyncio.Lock = asyncio.Lock()

        self.exchange_info = None

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            filename="arb_bot.log",
        )

    async def connect(self) -> None:
        """Initialise client, fetch pairs and balances."""
        self.client = await AsyncClient.create(self.api_key, self.api_secret)
        self.exchange_info = await self.client.get_exchange_info()
        self.bsm = BinanceSocketManager(self.client)

        await self.fetch_pairs()
        balance = await self.client.get_asset_balance(asset="EUR")
        self.balance_eur = Decimal(balance["free"])
        logging.info("Initial EUR balance = %s", self.balance_eur)

    async def fetch_pairs(self) -> None:
        """Fetch valid pairs and build EUR cycles."""
        assert self.client is not None
        info = await self.client.get_exchange_info()
        symbols = info["symbols"]
        pairs = []
        for s in symbols:
            if s["status"] != "TRADING":
                continue
            base, quote = s["baseAsset"], s["quoteAsset"]
            symbol = s["symbol"]
            pairs.append((base, quote, symbol))

        # Build triangular cycles starting/ending in EUR
        by_quote: Dict[str, List[Tuple[str, str]]] = {}
        by_base: Dict[str, List[Tuple[str, str]]] = {}
        for base, quote, symbol in pairs:
            by_quote.setdefault(quote, []).append((base, symbol))
            by_base.setdefault(base, []).append((quote, symbol))

        cycles: List[Tuple[str, str, str]] = []
        # EUR -> A (pair1 base=A, quote=EUR)
        for a, sym1 in by_quote.get("EUR", []):
            # A -> B (pair2 base=B, quote=A)
            for b, sym2 in by_quote.get(a, []):
                # B -> EUR (pair3 base=B, quote=EUR)
                for q, sym3 in by_base.get(b, []):
                    if q == "EUR":
                        cycles.append((sym1, sym2, sym3))
        self.cycles = cycles
        logging.info("Found %d EUR triangular cycles", len(cycles))

    async def start(self) -> None:
        """Start websocket and process messages using @bookTicker (bid/ask)."""
        assert self.bsm is not None
        # streams @bookTicker pour avoir bestBid/bestAsk
        streams = list({f"{s.lower()}@bookTicker" for cycle in self.cycles for s in cycle})
        if not streams:
            logging.error("No streams to subscribe")
            return

        while True:
            try:
                socket = self.bsm.multiplex_socket(streams)
                async with socket as stream:
                    while True:
                        msg = await stream.recv()
                        await self.handle_message(msg)
                        await asyncio.sleep(0)
            except Exception as e:
                logging.error("WS error, reconnecting in 2s: %s", e)
                await asyncio.sleep(2)

    async def handle_message(self, msg: dict) -> None:
        """Handle @bookTicker messages and look for arbitrage cycles."""
        data = msg.get("data", {})
        symbol = data.get("s")
        bid = data.get("b")
        ask = data.get("a")
        if symbol and bid and ask:
            try:
                self.book[symbol] = {
                    "bid": Decimal(bid),
                    "ask": Decimal(ask),
                }
            except Exception:
                return

        if self.in_trade.locked():
            return

        for cycle in self.cycles:
            if all(s in self.book for s in cycle):
                await self.check_cycle(cycle)

    def _simulate_cycle_net(self, eur_amount: Decimal, cycle: Tuple[str, str, str]) -> Tuple[Decimal, Dict[str, Decimal]]:
        """
        Simule le cycle avec ask/bid + frais. Retourne (EUR_final_net, details).
        Ordres:
          1) BUY A avec EUR au ask1
          2) BUY B avec A (quote=A) au ask2
          3) SELL B contre EUR au bid3
        """
        sym1, sym2, sym3 = cycle
        ask1 = self.book[sym1]["ask"]
        ask2 = self.book[sym2]["ask"]
        bid3 = self.book[sym3]["bid"]

        # 1) EUR -> A (market buy, taker)
        A = (eur_amount / ask1) * (Decimal("1") - TAKER_FEE)
        # 2) A -> B (market buy, quote = A)
        B = (A / ask2) * (Decimal("1") - TAKER_FEE)
        # 3) B -> EUR (market sell)
        EUR_final = (B * bid3) * (Decimal("1") - TAKER_FEE)

        details = {
            "ask1": ask1, "ask2": ask2, "bid3": bid3,
            "A": A, "B": B, "EUR_final": EUR_final
        }
        return EUR_final, details

    async def check_cycle(self, cycle: Tuple[str, str, str]) -> None:
        """Évalue un cycle avec bookTicker + frais, applique seuils/minNotional, puis lance l'exécution."""
        amount_eur = self.balance_eur
        if amount_eur <= 0:
            return

        # Simulation du profit net (avec frais)
        eur_out, details = self._simulate_cycle_net(amount_eur, cycle)
        profit = eur_out - amount_eur
        
        if profit <= 0:
            return
        profit_pct = profit / amount_eur
        logging.info("After simulation profite %s => EUR OUT :  %s EUR",
                         (profit / amount_eur), eur_out)

        

        # Seuils (net + absolu)
        if profit_pct < MIN_PROFIT_PCT:
            return

        # Vérif minNotional basique (théorique) avant de tenter
        # 1) sym1 : quote = EUR
        _, _, min_notional1 = extract_filters(self.exchange_info, cycle[0])
        if amount_eur < (min_notional1 * NOTIONAL_MARGIN):
            logging.info("Skip %s: NOTIONAL (1) needed >= %s EUR, have %s EUR",
                         cycle[0], min_notional1, amount_eur)
            return

        # 2) sym2 : quote = A, on utilise le montant A théorique
        _, _, min_notional2 = extract_filters(self.exchange_info, cycle[1])
        amount_a_theoretical = details["A"]
        if amount_a_theoretical < (min_notional2 * NOTIONAL_MARGIN):
            logging.info("Skip %s: NOTIONAL (2) needed >= %s A, have %s A",
                         cycle[1], min_notional2, amount_a_theoretical)
            return

        # 3) sym3 : SELL qty = B (minNotional sera revalidé après arrondi)
        step3, min_qty3, min_notional3 = extract_filters(self.exchange_info, cycle[2])
        sell_qty_theoretical = details["B"]
        sell_qty_theoretical = round_step(sell_qty_theoretical, step3)
        notional3 = sell_qty_theoretical * self.book[cycle[2]]["bid"]
        if sell_qty_theoretical < min_qty3 or notional3 < (min_notional3 * NOTIONAL_MARGIN):
            logging.info("Skip %s: NOTIONAL (3) needed >= %s, have %s (qty=%s, step=%s, min_qty=%s)",
                         cycle[2], min_notional3, notional3, sell_qty_theoretical, step3, min_qty3)
            return

        # OK, on tente l'exécution réelle
        await self.execute_cycle(cycle)

    async def _get_free(self, asset: str) -> Decimal:
        bal = await self.client.get_asset_balance(asset=asset)
        return Decimal(bal["free"])

    async def _safe_order(self, **kwargs):
        """create_order avec petits retries/backoff."""
        assert self.client is not None
        for attempt in range(3):
            try:
                return await self.client.create_order(**kwargs)
            except BinanceAPIException as e:
                logging.error("Order error (attempt %s): code=%s msg=%s kwargs=%s",
                              attempt+1, e.code, getattr(e, "message", str(e)), kwargs)
                await asyncio.sleep(0.4 * (2 ** attempt))
            except Exception as e:
                logging.error("Order exception (attempt %s): %s", attempt+1, e)
                await asyncio.sleep(0.4 * (2 ** attempt))
        raise RuntimeError("Order failed after retries")

    async def execute_cycle(
        self,
        cycle: Tuple[str, str, str],
    ) -> None:
        """Exécute un cycle avec relcture des soldes réels et buffers pour éviter -2010."""
        async with self.in_trade:
            if not self.dry_run and self.client:
                try:
                    # --- 1) BUY A avec EUR (sym1) ---
                    base1, quote1 = get_assets(self.exchange_info, cycle[0])  # base=A, quote=EUR
                    eur_to_spend = self.balance_eur * BALANCE_BUFFER
                    _, _, min_notional1 = extract_filters(self.exchange_info, cycle[0])
                    if eur_to_spend < (min_notional1 * NOTIONAL_MARGIN):
                        logging.info("Skip %s: provided EUR %s < minNotional %s",
                                     cycle[0], eur_to_spend, min_notional1)
                        return

                    await self._safe_order(
                        symbol=cycle[0],
                        side="BUY",
                        type="MARKET",
                        quoteOrderQty=fmt_decimal(eur_to_spend)
                    )

                    # Relire A
                    free_a = await self._get_free(base1)
                    if free_a <= 0:
                        logging.info("Skip %s: no A received after BUY", cycle[0])
                        return

                    # --- 2) BUY B avec A comme quote (sym2) ---
                    base2, quote2 = get_assets(self.exchange_info, cycle[1])  # base=B, quote=A
                    a_to_spend = free_a * BALANCE_BUFFER
                    _, _, min_notional2 = extract_filters(self.exchange_info, cycle[1])
                    if a_to_spend < (min_notional2 * NOTIONAL_MARGIN):
                        logging.info("Skip %s: provided A %s < minNotional %s",
                                     cycle[1], a_to_spend, min_notional2)
                        return

                    await self._safe_order(
                        symbol=cycle[1],
                        side="BUY",
                        type="MARKET",
                        quoteOrderQty=fmt_decimal(a_to_spend)
                    )

                    # Relire B
                    free_b = await self._get_free(base2)
                    if free_b <= 0:
                        logging.info("Skip %s: no B received after BUY", cycle[1])
                        return

                    # --- 3) SELL B contre EUR (sym3) ---
                    step3, min_qty3, min_notional3 = extract_filters(self.exchange_info, cycle[2])
                    qty_b = round_step(free_b * BALANCE_BUFFER, step3)
                    if qty_b < min_qty3:
                        logging.info("Skip %s: qty %s < minQty %s", cycle[2], qty_b, min_qty3)
                        return

                    bid3 = self.book[cycle[2]]["bid"]
                    if (qty_b * bid3) < (min_notional3 * NOTIONAL_MARGIN):
                        logging.info("Skip %s: notional %s < minNotional %s",
                                     cycle[2], qty_b * bid3, min_notional3)
                        return

                    await self._safe_order(
                        symbol=cycle[2],
                        side="SELL",
                        type="MARKET",
                        quantity=fmt_decimal(qty_b)
                    )

                except Exception as exc:
                    logging.error("Trade execution failed: %s", exc)
                    return

            # Mettre à jour le PnL (approximation : relire EUR)
            try:
                eur_after = await self._get_free("EUR")
            except Exception:
                eur_after = self.balance_eur  # fallback

            before = self.balance_eur
            profit = eur_after - before
            self.balance_eur = eur_after
            self.cumulative_profit += profit

            logging.info(
                "Cycle %s executed: before=%s EUR after=%s EUR profit=%s EUR cum_profit=%s",
                cycle, before, eur_after, profit, self.cumulative_profit,
            )
            await self.send_telegram_alert(before, eur_after, profit)

    async def send_telegram_alert(
        self, invested: Decimal, final: Decimal, profit: Decimal
    ) -> None:
        if not self.telegram_token or not self.telegram_chat_id:
            return
        text = (
            f"Invested: {invested:.2f} EUR\n"
            f"Final: {final:.2f} EUR\n"
            f"Profit: {profit:.2f} EUR\n"
            f"Cumulative: {self.cumulative_profit:.2f} EUR"
        )
        url = (
            f"https://api.telegram.org/bot{self.telegram_token}/sendMessage?"
            f"chat_id={self.telegram_chat_id}&text={quote_plus(text)}"
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    await resp.text()
        except Exception as exc:  # pragma: no cover - network issue
            logging.error("Telegram alert failed: %s", exc)

    async def run(self) -> None:
        await self.connect()
        await self.start()


if __name__ == "__main__":
    bot = ArbBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass
