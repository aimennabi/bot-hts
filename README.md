# bot-hts

Ce dépôt contient un bot d'arbitrage triangulaire pour Binance.

## Utilisation

1. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```
2. Définir les variables d'environnement nécessaires :
   - `BINANCE_API_KEY` et `BINANCE_SECRET_KEY` pour l'accès à l'API Binance.
   - Facultatif : `TELEGRAM_TOKEN` et `TELEGRAM_CHAT_ID` pour recevoir des alertes Telegram.
   - `DRY_RUN=0` pour exécuter réellement les transactions (par défaut `DRY_RUN=1` simule seulement).
3. Lancer le bot :
   ```bash
   python arb_eur_bot_class.py
   ```
