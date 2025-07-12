#!/bin/bash

# ========================================
# FUSIONNER TOUS LES HTML EN UN SEUL
# ========================================

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
#DEST_PATH="database/africa/south_africa/johannesburg/merged_data"
DEST_PATH="database/africa/south_africa/johannesburg/johannesburg_custom_all"

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}=== Fusion des HTML en fichiers uniques ===${NC}"

mkdir -p temp/html_merge

# ========================================
# 1. CRÉER UN SEUL FICHIER CARD_MAIN.HTML
# ========================================
echo -e "\n${BLUE}1. Création du fichier card_main unifié...${NC}"

cat > temp/html_merge/all_cards_main.html << 'HTML_EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Johannesburg - Toutes les Cartes</title>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 0;
            padding: 0;
            background: #f5f5f5;
        }
        .header {
            background: #333;
            color: white;
            padding: 20px;
            text-align: center;
            position: sticky;
            top: 0;
            z-index: 1000;
        }
        .navigation {
            background: #444;
            padding: 10px;
            text-align: center;
            position: sticky;
            top: 60px;
            z-index: 999;
        }
        .navigation a {
            color: white;
            margin: 0 10px;
            text-decoration: none;
            padding: 5px 10px;
            border-radius: 3px;
            background: #666;
        }
        .navigation a:hover {
            background: #888;
        }
        .ward-section {
            background: white;
            margin: 20px;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .ward-title {
            background: #4CAF50;
            color: white;
            padding: 10px;
            margin: -20px -20px 20px -20px;
            border-radius: 8px 8px 0 0;
        }
        .iframe-container {
            width: 100%;
            height: 600px;
            border: 1px solid #ddd;
            border-radius: 4px;
            overflow: hidden;
        }
        iframe {
            width: 100%;
            height: 100%;
            border: none;
        }
        .no-data {
            padding: 40px;
            text-align: center;
            color: #999;
            background: #f9f9f9;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Johannesburg - Consolidation des Cartes</h1>
        <p>Toutes les cartes principales des 135 wards</p>
    </div>
    
    <div class="navigation">
        <a href="#ward-1">Ward 1</a>
        <a href="#ward-50">Ward 50</a>
        <a href="#ward-100">Ward 100</a>
        <a href="#ward-135">Ward 135</a>
    </div>
HTML_EOF

# Télécharger et intégrer chaque carte
card_count=0
for i in {1..135}; do
    echo -ne "\rTraitement carte ward $i/135..."
    
    card_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/card_main.html"
    
    echo "<div class='ward-section' id='ward-$i'>" >> temp/html_merge/all_cards_main.html
    echo "  <h2 class='ward-title'>Ward $i</h2>" >> temp/html_merge/all_cards_main.html
    
    if gcloud storage ls "$card_file" &>/dev/null; then
        # Télécharger le contenu
        gcloud storage cp "$card_file" "temp/html_merge/temp_card.html" -q
        
        # Extraire seulement le contenu du body (éviter les conflits de styles)
        if [ -f "temp/html_merge/temp_card.html" ]; then
            echo "  <div class='iframe-container'>" >> temp/html_merge/all_cards_main.html
            # Utiliser un iframe pour isoler les styles
            echo "    <iframe srcdoc='" >> temp/html_merge/all_cards_main.html
            # Échapper les quotes pour l'attribut srcdoc
            sed "s/'/\&#39;/g" temp/html_merge/temp_card.html >> temp/html_merge/all_cards_main.html
            echo "'></iframe>" >> temp/html_merge/all_cards_main.html
            echo "  </div>" >> temp/html_merge/all_cards_main.html
            ((card_count++))
        fi
        rm -f temp/html_merge/temp_card.html
    else
        echo "  <div class='no-data'>Pas de carte disponible pour ce ward</div>" >> temp/html_merge/all_cards_main.html
    fi
    
    echo "</div>" >> temp/html_merge/all_cards_main.html
done

# Fermer le HTML
echo "</body></html>" >> temp/html_merge/all_cards_main.html

echo -e "\n${GREEN}✓ Fichier card_main unifié créé : $card_count cartes intégrées${NC}"

# ========================================
# 2. CRÉER UN SEUL FICHIER OSM_MAP.HTML
# ========================================
echo -e "\n${BLUE}2. Création du fichier OSM map unifié...${NC}"

cat > temp/html_merge/all_OSM_maps.html << 'HTML_EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Johannesburg - Toutes les Cartes OSM</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>
        body { margin: 0; padding: 0; }
        #map { height: 100vh; width: 100%; }
        .ward-control {
            background: white;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.3);
        }
        .ward-control select {
            padding: 5px;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        // Initialiser la carte centrée sur Johannesburg
        var map = L.map('map').setView([-26.2041, 28.0473], 11);
        
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(map);
        
        // Conteneur pour tous les layers des wards
        var wardLayers = {};
        var allNodes = [];
HTML_EOF

# Collecter tous les nœuds OSM
osm_count=0
for i in {1..135}; do
    echo -ne "\rCollecte des données OSM ward $i/135..."
    
    osm_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/OSM_map_merged.json"
    
    if gcloud storage ls "$osm_file" &>/dev/null; then
        # Télécharger et extraire les nœuds
        gcloud storage cp "$osm_file" - -q | jq -c '.nodes[]' | while read node; do
            echo "        allNodes.push({ward: $i, node: $node});" >> temp/html_merge/all_OSM_maps.html
        done
        ((osm_count++))
    fi
done

# Continuer le script JavaScript
cat >> temp/html_merge/all_OSM_maps.html << 'HTML_EOF'
        
        // Grouper les nœuds par ward
        var nodesByWard = {};
        allNodes.forEach(function(item) {
            if (!nodesByWard[item.ward]) {
                nodesByWard[item.ward] = [];
            }
            nodesByWard[item.ward].push(item.node);
        });
        
        // Créer un layer pour chaque ward
        Object.keys(nodesByWard).forEach(function(ward) {
            var wardLayer = L.layerGroup();
            
            nodesByWard[ward].forEach(function(node) {
                if (node.lat && node.lon) {
                    L.circleMarker([node.lat, node.lon], {
                        radius: 3,
                        fillColor: "#ff7800",
                        color: "#000",
                        weight: 1,
                        opacity: 1,
                        fillOpacity: 0.8
                    }).bindPopup('Ward ' + ward + '<br>ID: ' + node.id).addTo(wardLayer);
                }
            });
            
            wardLayers['Ward ' + ward] = wardLayer;
        });
        
        // Ajouter tous les layers à la carte
        Object.values(wardLayers).forEach(function(layer) {
            layer.addTo(map);
        });
        
        // Contrôle des layers
        L.control.layers(null, wardLayers, {collapsed: false}).addTo(map);
        
        // Ajuster la vue pour inclure tous les points
        if (allNodes.length > 0) {
            var bounds = L.latLngBounds(allNodes.map(function(item) {
                return [item.node.lat, item.node.lon];
            }));
            map.fitBounds(bounds);
        }
    </script>
</body>
</html>
HTML_EOF

echo -e "\n${GREEN}✓ Fichier OSM map unifié créé : $osm_count wards intégrés${NC}"

# ========================================
# 3. CRÉER UN SEUL FICHIER SV_MAP.HTML
# ========================================
echo -e "\n${BLUE}3. Création du fichier SV map unifié...${NC}"

# Structure similaire pour SV
cp temp/html_merge/all_OSM_maps.html temp/html_merge/all_SV_maps.html
sed -i 's/Cartes OSM/Cartes Street View/g' temp/html_merge/all_SV_maps.html
sed -i 's/OSM_map_merged/SV_map_merged/g' temp/html_merge/all_SV_maps.html
sed -i 's/#ff7800/#4CAF50/g' temp/html_merge/all_SV_maps.html

# ========================================
# 4. CRÉER UN DASHBOARD PRINCIPAL
# ========================================
echo -e "\n${BLUE}4. Création du dashboard principal...${NC}"

cat > temp/html_merge/dashboard.html << 'HTML_EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Johannesburg - Dashboard Consolidé</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #333;
            text-align: center;
            padding: 20px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .cards-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }
        .card h2 {
            color: #4CAF50;
            margin-bottom: 10px;
        }
        .card p {
            color: #666;
            margin-bottom: 20px;
        }
        .card a {
            display: inline-block;
            padding: 10px 20px;
            background: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            transition: background 0.3s;
        }
        .card a:hover {
            background: #45a049;
        }
        .stats {
            background: #333;
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-top: 20px;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Johannesburg - Dashboard de Consolidation</h1>
        
        <div class="cards-grid">
            <div class="card">
                <h2>📊 Cartes Principales</h2>
                <p>Toutes les cartes de couverture des 135 wards</p>
                <a href="all_cards_main.html">Voir les cartes</a>
            </div>
            
            <div class="card">
                <h2>🗺️ Cartes OSM</h2>
                <p>Visualisation de tous les nœuds OpenStreetMap</p>
                <a href="all_OSM_maps.html">Voir la carte OSM</a>
            </div>
            
            <div class="card">
                <h2>📍 Cartes Street View</h2>
                <p>Tous les points Street View disponibles</p>
                <a href="all_SV_maps.html">Voir la carte SV</a>
            </div>
        </div>
        
        <div class="stats">
            <h3>Statistiques</h3>
            <p>STATS_PLACEHOLDER</p>
        </div>
    </div>
</body>
</html>
HTML_EOF

# Remplacer les statistiques
stats_text="Wards: 135 | Cartes: $card_count | OSM: $osm_count | SV: $sv_count"
sed -i "s/STATS_PLACEHOLDER/$stats_text/g" temp/html_merge/dashboard.html

# ========================================
# 5. UPLOAD DES FICHIERS
# ========================================
echo -e "\n${BLUE}5. Upload des fichiers unifiés...${NC}"

gcloud storage cp temp/html_merge/dashboard.html "gs://${BUCKET}/${DEST_PATH}/dashboard.html"
gcloud storage cp temp/html_merge/all_cards_main.html "gs://${BUCKET}/${DEST_PATH}/all_cards_main.html"
gcloud storage cp temp/html_merge/all_OSM_maps.html "gs://${BUCKET}/${DEST_PATH}/all_OSM_maps.html"
gcloud storage cp temp/html_merge/all_SV_maps.html "gs://${BUCKET}/${DEST_PATH}/all_SV_maps.html"

# Nettoyer
rm -rf temp/html_merge

echo -e "\n${GREEN}✓ Fusion HTML terminée !${NC}"
echo ""
echo "Fichiers créés :"
echo "  • dashboard.html         - Page d'accueil"
echo "  • all_cards_main.html   - Toutes les cartes principales"
echo "  • all_OSM_maps.html     - Carte OSM globale interactive"
echo "  • all_SV_maps.html      - Carte SV globale interactive"
echo ""
echo "Accès : gs://${BUCKET}/${DEST_PATH}/dashboard.html"