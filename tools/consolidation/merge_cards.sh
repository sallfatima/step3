#!/bin/bash
# Extraire et consolider les statistiques des cartes

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/merged_data"

echo "=== Consolidation des cartes des 135 wards ==="

mkdir -p temp/cards

# Télécharger les cartes
echo "Téléchargement des cartes..."
count=0
for i in {1..135}; do
    echo -ne "\rWard $i/135..."
    card_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/card_main.html"
    if gcloud storage ls "$card_file" &>/dev/null; then
        gcloud storage cp "$card_file" "temp/cards/card_ward_${i}.html" -q
        ((count++))
    fi
done
echo -e "\n$count cartes trouvées"

# Extraire les statistiques et créer une carte consolidée
echo "Création de la carte consolidée..."
python3 << 'PYTHON_EOF'
import os
import re
from bs4 import BeautifulSoup
import json

def extract_card_stats(html_file):
    """Extrait les statistiques d'une carte HTML"""
    with open(html_file, 'r') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    # Ici vous devriez adapter selon le format exact de vos cartes
    # Exemple générique :
    stats = {
        'coverage': 0,
        'total_sv': 0,
        'total_osm': 0
    }
    
    # Chercher les valeurs dans le HTML (adapter selon votre format)
    # ...
    
    return stats

# Pour l'instant, créer une carte récapitulative simple
consolidated_stats = {
    'total_wards': count,
    'wards_with_cards': len([f for f in os.listdir('temp/cards') if f.endswith('.html')]),
    'consolidated': True
}

# Créer un HTML récapitulatif
html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Johannesburg - Carte Consolidée</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .stats {{ background: #f0f0f0; padding: 20px; border-radius: 5px; }}
        h1 {{ color: #333; }}
    </style>
</head>
<body>
    <h1>Johannesburg - Statistiques Consolidées</h1>
    <div class="stats">
        <h2>Résumé</h2>
        <p>Total des wards : {consolidated_stats['total_wards']}</p>
        <p>Wards avec cartes : {consolidated_stats['wards_with_cards']}</p>
        <p>Date de consolidation : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    <p>Pour générer une carte interactive complète, exécutez l'action 'card' sur la zone consolidée.</p>
</body>
</html>
"""

with open('temp/card_consolidated.html', 'w') as f:
    f.write(html_content)

print(f"Carte consolidée créée")
PYTHON_EOF

# Upload
gcloud storage cp "temp/card_consolidated.html" "gs://${BUCKET}/${DEST_PATH}/card_consolidated_summary.html"
echo "✓ Carte consolidée : gs://${BUCKET}/${DEST_PATH}/card_consolidated_summary.html"

rm -rf temp/cards