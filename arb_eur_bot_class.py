import os
import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Tuple

import aiohttp
from binance import AsyncClient, BinanceSocketManager
from decimal import ROUND_DOWN  # <-- ajoute ceci à côté de Decimal

def round_step(qty: Decimal, step: Decimal) -> Decimal:
    # arrondit vers le bas au multiple de step
    return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step

def extract_filters(info, symbol):
    # récupère LOT_SIZE.stepSize / minQty et (MIN_)NOTIONAL.minNotional
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
    # fallback
    return Decimal("0.00000001"), Decimal("0"), Decimal("0")



class ArbBot:
    """Triangular arbitrage bot starting/ending in EUR on Binance."""

    def __init__(self) -> None:
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_SECRET_KEY")
        self.telegram_token = os.getenv("TELEGRAM_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.dry_run = os.getenv("DRY_RUN", "1") == "1"

        self.client: AsyncClient | None = None
        self.bsm: BinanceSocketManager | None = None

        self.prices: Dict[str, Decimal] = {}
        self.cycles: List[Tuple[str, str, str]] = []
        self.balance_eur: Decimal = Decimal("0")
        self.cumulative_profit: Decimal = Decimal("0")
        self.in_trade: asyncio.Lock = asyncio.Lock()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            filename="arb_bot.log",
        )

    async def connect(self) -> None:
        """Initialise client, fetch pairs and balances."""
        self.client = await AsyncClient.create(self.api_key, self.api_secret)
        self.bsm = BinanceSocketManager(self.client)
        self.exchange_info = await self.client.get_exchange_info()
        await self.fetch_pairs()
        balance = await self.client.get_asset_balance(asset="EUR")
        self.balance_eur = Decimal(balance["free"])

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

    async def start(self) -> None:
        """Start websocket and process messages."""
        assert self.bsm is not None
        streams = list({f"{s.lower()}@ticker" for cycle in self.cycles for s in cycle})
        socket = self.bsm.multiplex_socket(streams)
        async with socket as stream:
            while True:
                msg = await stream.recv()
                await self.handle_message(msg)
                await asyncio.sleep(0)  # yield control

    async def handle_message(self, msg: dict) -> None:
        """Handle incoming ticker messages and look for arbitrage cycles."""
        data = msg.get("data", {})
        symbol = data.get("s")
        price = data.get("c")
        if symbol and price:
            self.prices[symbol] = Decimal(price)

        if self.in_trade.locked():
            return

        for cycle in self.cycles:
            if all(s in self.prices for s in cycle):
                await self.check_cycle(cycle)

    async def check_cycle(self, cycle: Tuple[str, str, str]) -> None:
        """Evaluate a single cycle for arbitrage opportunity."""
        p1, p2, p3 = (self.prices[s] for s in cycle)
        amount_eur = self.balance_eur
        if amount_eur <= 0:
            return
        amount_a = amount_eur / p1
        amount_b = amount_a / p2
        final_eur = amount_b * p3
        profit = final_eur - amount_eur
        if profit <= 0:
            return
        profit_pct = profit / amount_eur
        if profit_pct > Decimal("0.01"):
            await self.execute_cycle(cycle, amount_a, amount_b, final_eur, profit)

    async def execute_cycle(
        self,
        cycle: Tuple[str, str, str],
        amount_a: Decimal,
        amount_b: Decimal,
        final_eur: Decimal,
        profit: Decimal,
    ) -> None:
        """Execute or simulate a profitable cycle."""
        async with self.in_trade:
            # --- Vérifs filtres Binance pour éviter -1013 NOTIONAL ---
            p1, p2, p3 = (self.prices[s] for s in cycle)
            # 1er ordre (BUY, quote = EUR)
            step1, min_qty1, min_notional1 = extract_filters(self.exchange_info, cycle[0])
            if self.balance_eur < (min_notional1 * Decimal("1.02")):  # marge 2%
                logging.info("Skip cycle %s: notional too low for %s", cycle, cycle[0])
                return

            # 2e ordre (BUY, quote = A -> amount_a)
            step2, min_qty2, min_notional2 = extract_filters(self.exchange_info, cycle[1])
            if amount_a < (min_notional2 * Decimal("1.02")):
                logging.info("Skip cycle %s: notional too low for %s", cycle, cycle[1])
                return

            # 3e ordre (SELL, quantity = B -> arrondi au step)
            step3, min_qty3, min_notional3 = extract_filters(self.exchange_info, cycle[2])
            sell_qty = round_step(amount_b, step3)
            if sell_qty < min_qty3 or (sell_qty * p3) < (min_notional3 * Decimal("1.02")):
                logging.info("Skip cycle %s: notional too low for %s (after rounding)", cycle, cycle[2])
                return
            #        ----------------------------------------------------------
            if not self.dry_run and self.client:
                try:
                    await self.client.create_order(
                        symbol=cycle[0],
                        side="BUY",
                        type="MARKET",
                        quoteOrderQty=str(self.balance_eur),
                    )
                    await self.client.create_order(
                        symbol=cycle[1],
                        side="BUY",
                        type="MARKET",
                        quoteOrderQty=str(amount_a),
                    )
                    await self.client.create_order(
                        symbol=cycle[2],
                        side="SELL",
                        type="MARKET",
                        quantity=str(sell_qty),
                    )
                except Exception as exc:  # pragma: no cover - network issue
                    logging.error("Trade execution failed: %s", exc)
                    return
            before = self.balance_eur
            self.balance_eur = final_eur
            self.cumulative_profit += profit
            logging.info(
                "Cycle %s executed: invest=%s final=%s profit=%s cum_profit=%s",
                cycle,
                before,
                final_eur,
                profit,
                self.cumulative_profit,
            )
            await self.send_telegram_alert(before, final_eur, profit)

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
            f"chat_id={self.telegram_chat_id}&text={aiohttp.helpers.quote(text)}"
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
