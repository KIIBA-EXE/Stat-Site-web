# Stat-DLB — Sync Google Search Console to Notion

Ce projet synchronise automatiquement les données Google Search Console (GSC) vers une base de données Notion.

## 1) Préparer Notion
Créez une base de données et partagez-la avec votre Intégration Notion.

Schéma de propriétés (en français) à créer dans la base Notion:
- Clé (Title) — clé composite unique = `${date}|${query}|${page}|${country}|${device}`
- Date (Date)
- Requête (Rich text)
- Page (URL)
- Pays (Select)
- Appareil (Select)
- Clics (Number)
- Impressions (Number)
- CTR (Number)
- Position (Number)

Astuce: si vous aviez déjà des propriétés en anglais, renommez-les pour correspondre exactement aux noms ci-dessus.

## 2) Préparer Google Cloud / Search Console
- API Search Console activée
- Service Account créé et clé JSON téléchargée
- Ajouter l’email du Service Account comme « Utilisateur avec droits complets » sur la propriété GSC
- Notez la valeur `siteUrl`:
  - Propriété URL-prefix: par ex. `https://danslesbottes.fr/`
  - Propriété Domain: `sc-domain:danslesbottes.fr`

## 3) Configurer le projet
- Placez la clé JSON du Service Account dans `./gcp-service-account.json` (ou changez le chemin via `.env`).
- Copiez `.env.example` en `.env` et complétez les valeurs.

## 4) Installation
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 5) Exécuter
Synchroniser les 3 derniers jours (J-3 à J-1):
```
python src/gsc_to_notion.py --site-url "https://danslesbottes.fr/" --days-back 3 --lag-days 2
```

Backfill 90 jours:
```
python src/gsc_to_notion.py --site-url "https://danslesbottes.fr/" --start 2025-05-16 --end 2025-08-13
```

## 6) Planification (cron)
```
crontab -e
# Tous les jours à 03:30 UTC (~05:30 Paris l’été)
30 3 * * * cd /chemin/vers/Stat-Dlb && . .venv/bin/activate && python src/gsc_to_notion.py --site-url "https://danslesbottes.fr/" --days-back 3 --lag-days 2 >> cron.log 2>&1
```

## Options du script
- `--site-url` (requis): `https://.../` ou `sc-domain:...`
- `--start` / `--end`: YYYY-MM-DD
- `--days-back`: fenêtre en jours à partir d’aujourd’hui (excluant les `lag-days`)
- `--lag-days`: latence GSC (par défaut 2)
- `--country`, `--device`: filtres optionnels
- `--row-limit`: taille des pages GSC (par défaut 25000)

## Variables d’environnement (.env)
- `NOTION_TOKEN=` token d’intégration
- `NOTION_DATABASE_ID=` id de la base
- `GOOGLE_SERVICE_ACCOUNT_JSON=` chemin vers le JSON (ex: `gcp-service-account.json`)
- `NOTION_RATE_LIMIT_PER_SEC=3` (facultatif)

## Upsert / déduplication
Le script calcule une propriété `Clé` et fait un upsert: s’il trouve une page Notion avec la même valeur de Clé, il met à jour; sinon il crée. Assurez-vous que la propriété `Clé` est de type `Title`.

## Sécurité
Ne versionnez jamais votre clé JSON ni votre `.env`.
