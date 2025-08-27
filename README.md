# bot-hts

Ce dépôt contient un bot d'arbitrage triangulaire pour Binance.

## Utilisation

1. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```
2. Définir les variables d'environnement nécessaires :

   Exporter les clés dans votre terminal avant de lancer le bot :

   ```bash
   export BINANCE_API_KEY="votre_cle"
   export BINANCE_SECRET_KEY="votre_secret"
   # Facultatif :
   export TELEGRAM_TOKEN="votre_token"
   export TELEGRAM_CHAT_ID="votre_chat_id"
   export DRY_RUN=1  # passer à 0 pour exécuter réellement les transactions
   ```

   Dans GitHub Actions, ajoutez ces mêmes variables comme secrets via **Settings > Secrets and variables > Actions** pour permettre au workflow de déployer le bot.
3. Lancer le bot :
   ```bash
   python arb_eur_bot_class.py
   ```
