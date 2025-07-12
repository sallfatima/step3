#!/bin/bash

# ========================================
# SCRIPT COMPLET DE FUSION JOHANNESBURG
# ========================================

# Configuration
BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/johannesburg_merged_data"
TOTAL_WARDS=135

# Couleurs
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Variables globales pour les statistiques
TOTAL_IMAGES=0
TOTAL_ANNOTATIONS=0
TOTAL_OSM_NODES=0
TOTAL_SV_NODES=0
TOTAL_PREDICTIONS=0

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}   FUSION COMPLETE JOHANNESBURG${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""
echo "Ce script va fusionner :"
echo "  ✓ Images"
echo "  ✓ Annotations (avec dédoublonnage)"
echo "  ✓ Graphes OSM"
echo "  ✓ Graphes Street View"
echo "  ✓ Prédictions"
echo "  ✓ Cartes"
echo "  ✓ Fichiers Excel/CSV des deliverables"
echo ""
read -p "Continuer ? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 0
fi

# # Créer la structure de destination
# echo -e "\n${BLUE}1. Création de la structure...${NC}"
# gcloud storage mkdir -p "gs://${BUCKET}/${DEST_PATH}"
# gcloud storage mkdir -p "gs://${BUCKET}/${DEST_PATH}/images"
# gcloud storage mkdir -p "gs://${BUCKET}/${DEST_PATH}/annotations"
# gcloud storage mkdir -p "gs://${BUCKET}/${DEST_PATH}/predictions"
# gcloud storage mkdir -p "gs://${BUCKET}/${DEST_PATH}/graphs"
# gcloud storage mkdir -p "gs://${BUCKET}/${DEST_PATH}/deliverables"
# gcloud storage mkdir -p "gs://${BUCKET}/${DEST_PATH}/stats"

# # Créer les dossiers temporaires
# mkdir -p temp/{annotations,osm,sv,excel,stats}

# ========================================
# VERIFIER LES DEPENDANCES
# ========================================
echo -e "\n${BLUE}2. Vérification des dépendances...${NC}"

# Vérifier jq
if ! command -v jq &> /dev/null; then
    echo "Installation de jq..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install jq
    else
        sudo apt-get update && sudo apt-get install -y jq
    fi
fi

# Vérifier Python et pandas
if python3 -c "import pandas" &>/dev/null; then
    PANDAS_AVAILABLE=true
    echo "✓ Python avec pandas disponible"
else
    PANDAS_AVAILABLE=false
    echo "⚠ Pandas non disponible - fusion Excel basique sera utilisée"
fi

# ========================================
# COMPTER LES IMAGES
# ========================================
echo -e "\n${BLUE}3. Comptage des images...${NC}"

for i in {1..135}; do
    echo -ne "\rWard $i/135..."
    count=$(gcloud storage ls "gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/images/*.jpg" 2>/dev/null | wc -l)
    TOTAL_IMAGES=$((TOTAL_IMAGES + count))
done
echo -e "\r${GREEN}✓ Total images : $TOTAL_IMAGES${NC}"

# ========================================
# FUSIONNER LES ANNOTATIONS
# ========================================
echo -e "\n${BLUE}4. Fusion des annotations...${NC}"

# Télécharger toutes les annotations
annotation_count=0
for i in {1..135}; do
    echo -ne "\rTéléchargement ward $i/135..."
    
    # Priorité aux fichiers sans doublons
    ann_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/annotations/annotations_no_duplicates_image_location.json"
    alt_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/annotations/annotations.json"
    
    if gcloud storage ls "$ann_file" &>/dev/null; then
        gcloud storage cp "$ann_file" "temp/annotations/ward_${i}.json" -q
        ((annotation_count++))
    elif gcloud storage ls "$alt_file" &>/dev/null; then
        gcloud storage cp "$alt_file" "temp/annotations/ward_${i}.json" -q
        ((annotation_count++))
    fi
done

echo -e "\n$annotation_count fichiers d'annotations trouvés"

if [ $annotation_count -gt 0 ]; then
    echo "Fusion des annotations..."
    
    # Si pandas disponible, utiliser Python
    if [ "$PANDAS_AVAILABLE" = true ]; then
        python3 << 'PYTHON_EOF'
import json
import os

merged = {
    "images": [],
    "annotations": [],
    "categories": []
}

image_id_offset = 0
annotation_id_offset = 0
categories_added = False
total_annotations = 0

files = sorted([f for f in os.listdir('temp/annotations') if f.endswith('.json')])

for filename in files:
    with open(os.path.join('temp/annotations', filename), 'r') as f:
        data = json.load(f)
    
    if not categories_added and 'categories' in data:
        merged['categories'] = data['categories']
        categories_added = True
    
    image_id_map = {}
    for img in data.get('images', []):
        old_id = img['id']
        new_id = old_id + image_id_offset
        image_id_map[old_id] = new_id
        img['id'] = new_id
        merged['images'].append(img)
    
    for ann in data.get('annotations', []):
        ann['id'] = ann['id'] + annotation_id_offset
        ann['image_id'] = image_id_map[ann['image_id']]
        merged['annotations'].append(ann)
        total_annotations += 1
    
    if data.get('images'):
        image_id_offset = max([img['id'] for img in merged['images']]) + 1
    if data.get('annotations'):
        annotation_id_offset = max([ann['id'] for ann in merged['annotations']]) + 1

with open('temp/annotations_merged.json', 'w') as f:
    json.dump(merged, f)

print(f"Total annotations fusionnées : {total_annotations}")
PYTHON_EOF
    else
        # Fusion basique avec jq
        echo '{"images": [], "annotations": [], "categories": []}' > temp/annotations_merged.json
        
        for file in temp/annotations/*.json; do
            if [ -f "$file" ]; then
                jq -s '.[0] as $merged | .[1] as $new | {
                    images: ($merged.images + $new.images),
                    annotations: ($merged.annotations + $new.annotations),
                    categories: (if ($merged.categories | length) == 0 then $new.categories else $merged.categories end)
                }' temp/annotations_merged.json "$file" > temp/merged_tmp.json
                mv temp/merged_tmp.json temp/annotations_merged.json
            fi
        done
    fi
    
    # Upload
    gcloud storage cp temp/annotations_merged.json "gs://${BUCKET}/${DEST_PATH}/annotations/annotations_all_wards.json"
    
    # Compter
    TOTAL_ANNOTATIONS=$(jq '.annotations | length' temp/annotations_merged.json)
    echo -e "${GREEN}✓ Annotations fusionnées : $TOTAL_ANNOTATIONS annotations${NC}"
fi

# ========================================
# FUSIONNER LES GRAPHES OSM
# ========================================
echo -e "\n${BLUE}5. Fusion des graphes OSM...${NC}"

osm_count=0
echo '{"nodes": [], "adjacency": []}' > temp/osm_merged.json

for i in {1..135}; do
    echo -ne "\rTraitement ward $i/135..."
    
    osm_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/osm_merged.json"
    
    if gcloud storage ls "$osm_file" &>/dev/null; then
        gcloud storage cp "$osm_file" "temp/osm/osm_${i}.json" -q
        
        # Fusionner
        jq -s '.[0] as $merged | .[1] as $new | {
            nodes: ($merged.nodes + ($new.nodes // [])),
            adjacency: ($merged.adjacency + ($new.adjacency // []))
        }' temp/osm_merged.json "temp/osm/osm_${i}.json" > temp/osm_tmp.json
        
        mv temp/osm_tmp.json temp/osm_merged.json
        rm "temp/osm/osm_${i}.json"
        ((osm_count++))
    fi
done

if [ $osm_count -gt 0 ]; then
    TOTAL_OSM_NODES=$(jq '.nodes | length' temp/osm_merged.json)
    gcloud storage cp temp/osm_merged.json "gs://${BUCKET}/${DEST_PATH}/graphs/osm_merged_all.json"
    echo -e "\n${GREEN}✓ Graphes OSM fusionnés : $osm_count fichiers, $TOTAL_OSM_NODES nœuds${NC}"
else
    echo -e "\n${RED}✗ Aucun graphe OSM trouvé${NC}"
fi

# ========================================
# FUSIONNER LES GRAPHES SV
# ========================================
echo -e "\n${BLUE}6. Fusion des graphes Street View...${NC}"

sv_count=0
echo '{"nodes": [], "adjacency": []}' > temp/sv_merged.json

for i in {1..135}; do
    echo -ne "\rTraitement ward $i/135..."
    
    sv_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/sv_merged.json"
    
    if gcloud storage ls "$sv_file" &>/dev/null; then
        gcloud storage cp "$sv_file" "temp/sv/sv_${i}.json" -q
        
        jq -s '.[0] as $merged | .[1] as $new | {
            nodes: ($merged.nodes + ($new.nodes // [])),
            adjacency: ($merged.adjacency + ($new.adjacency // []))
        }' temp/sv_merged.json "temp/sv/sv_${i}.json" > temp/sv_tmp.json
        
        mv temp/sv_tmp.json temp/sv_merged.json
        rm "temp/sv/sv_${i}.json"
        ((sv_count++))
    fi
done

if [ $sv_count -gt 0 ]; then
    TOTAL_SV_NODES=$(jq '.nodes | length' temp/sv_merged.json)
    gcloud storage cp temp/sv_merged.json "gs://${BUCKET}/${DEST_PATH}/graphs/sv_merged_all.json"
    echo -e "\n${GREEN}✓ Graphes SV fusionnés : $sv_count fichiers, $TOTAL_SV_NODES points${NC}"
else
    echo -e "\n${RED}✗ Aucun graphe SV trouvé${NC}"
fi

# ========================================
# COMPTER LES PREDICTIONS
# ========================================
echo -e "\n${BLUE}7. Comptage des prédictions...${NC}"

for i in {1..135}; do
    echo -ne "\rWard $i/135..."
    count=$(gcloud storage ls "gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/predictions/*.jpg" 2>/dev/null | wc -l)
    TOTAL_PREDICTIONS=$((TOTAL_PREDICTIONS + count))
done
echo -e "\r${GREEN}✓ Total prédictions : $TOTAL_PREDICTIONS${NC}"

# ========================================
# FUSIONNER LES DELIVERABLES
# ========================================
echo -e "\n${BLUE}8. Fusion des fichiers Excel/CSV...${NC}"

excel_count=0
csv_count=0

for i in {1..135}; do
    echo -ne "\rRecherche ward $i/135..."
    
    # Excel
    excel_files=$(gcloud storage ls "gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/deliverables/*.xlsx" 2>/dev/null || true)
    for file in $excel_files; do
        if [ ! -z "$file" ]; then
            filename=$(basename "$file")
            gcloud storage cp "$file" "temp/excel/ward_${i}_${filename}" -q
            ((excel_count++))
        fi
    done
    
    # CSV
    csv_files=$(gcloud storage ls "gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/deliverables/*.csv" 2>/dev/null || true)
    for file in $csv_files; do
        if [ ! -z "$file" ]; then
            filename=$(basename "$file")
            gcloud storage cp "$file" "temp/excel/ward_${i}_${filename}" -q
            ((csv_count++))
        fi
    done
done

echo -e "\n${GREEN}✓ Fichiers trouvés : $excel_count Excel, $csv_count CSV${NC}"

# Fusionner les CSV de manière simple
if [ $csv_count -gt 0 ]; then
    echo "Fusion des CSV..."
    first=true
    
    for file in temp/excel/*.csv; do
        if [ -f "$file" ]; then
            if [ "$first" = true ]; then
                head -1 "$file" > temp/deliverables_merged.csv
                echo ",ward" >> temp/deliverables_merged.csv
                first=false
            fi
            ward=$(basename "$file" | cut -d'_' -f2)
            tail -n +2 "$file" | sed "s/$/,$ward/" >> temp/deliverables_merged.csv
        fi
    done
    
    gcloud storage cp temp/deliverables_merged.csv "gs://${BUCKET}/${DEST_PATH}/deliverables/all_wards.csv"
fi

# ========================================
# GENERER LES STATISTIQUES
# ========================================
echo -e "\n${BLUE}9. Génération des statistiques...${NC}"

# Créer le fichier de stats
cat > temp/stats/consolidation_stats.json << EOF
{
    "consolidation_date": "$(date -u +%Y-%m-%d\ %H:%M:%S) UTC",
    "total_wards": $TOTAL_WARDS,
    "statistics": {
        "images": {
            "total": $TOTAL_IMAGES,
            "average_per_ward": $(echo "scale=2; $TOTAL_IMAGES / $TOTAL_WARDS" | bc)
        },
        "annotations": {
            "total": $TOTAL_ANNOTATIONS,
            "files_merged": $annotation_count
        },
        "graphs": {
            "osm_files_merged": $osm_count,
            "osm_total_nodes": $TOTAL_OSM_NODES,
            "sv_files_merged": $sv_count,
            "sv_total_nodes": $TOTAL_SV_NODES
        },
        "predictions": {
            "total": $TOTAL_PREDICTIONS
        },
        "deliverables": {
            "excel_files": $excel_count,
            "csv_files": $csv_count
        }
    }
}
EOF

gcloud storage cp temp/stats/consolidation_stats.json "gs://${BUCKET}/${DEST_PATH}/stats/consolidation_stats.json"

# ========================================
# CREER UN RAPPORT HTML
# ========================================
echo -e "\n${BLUE}10. Création du rapport...${NC}"

cat > temp/consolidation_report.html << EOF
<!DOCTYPE html>
<html>
<head>
    <title>Johannesburg - Rapport de Consolidation</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }
        h2 { color: #666; margin-top: 30px; }
        .stat-box { background: #f9f9f9; padding: 15px; margin: 10px 0; border-left: 4px solid #4CAF50; }
        .number { font-size: 24px; font-weight: bold; color: #4CAF50; }
        .label { color: #666; font-size: 14px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
        .success { color: #4CAF50; }
        .warning { color: #ff9800; }
        .error { color: #f44336; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Johannesburg - Rapport de Consolidation</h1>
        <p>Date : $(date)</p>
        
        <h2>Vue d'ensemble</h2>
        <div class="grid">
            <div class="stat-box">
                <div class="number">$TOTAL_WARDS</div>
                <div class="label">Wards traités</div>
            </div>
            <div class="stat-box">
                <div class="number">$TOTAL_IMAGES</div>
                <div class="label">Images totales</div>
            </div>
            <div class="stat-box">
                <div class="number">$TOTAL_ANNOTATIONS</div>
                <div class="label">Annotations</div>
            </div>
            <div class="stat-box">
                <div class="number">$TOTAL_PREDICTIONS</div>
                <div class="label">Prédictions</div>
            </div>
        </div>
        
        <h2>Graphes</h2>
        <div class="grid">
            <div class="stat-box">
                <div class="number">$TOTAL_OSM_NODES</div>
                <div class="label">Nœuds OSM</div>
                <div class="label">($osm_count fichiers fusionnés)</div>
            </div>
            <div class="stat-box">
                <div class="number">$TOTAL_SV_NODES</div>
                <div class="label">Points Street View</div>
                <div class="label">($sv_count fichiers fusionnés)</div>
            </div>
        </div>
        
        <h2>Fichiers créés</h2>
        <ul>
            <li class="success">✓ Annotations fusionnées : annotations_all_wards.json</li>
            <li class="$([ $osm_count -gt 0 ] && echo 'success' || echo 'error')">$([ $osm_count -gt 0 ] && echo '✓' || echo '✗') Graphe OSM : osm_merged_all.json</li>
            <li class="$([ $sv_count -gt 0 ] && echo 'success' || echo 'error')">$([ $sv_count -gt 0 ] && echo '✓' || echo '✗') Graphe SV : sv_merged_all.json</li>
            <li class="$([ $csv_count -gt 0 ] && echo 'success' || echo 'error')">$([ $csv_count -gt 0 ] && echo '✓' || echo '✗') Deliverables : all_wards.csv</li>
        </ul>
        
        <h2>Chemin de stockage</h2>
        <code>gs://${BUCKET}/${DEST_PATH}/</code>
    </div>
</body>
</html>
EOF

gcloud storage cp temp/consolidation_report.html "gs://${BUCKET}/${DEST_PATH}/consolidation_report.html"

# ========================================
# NETTOYAGE
# ========================================
echo -e "\n${BLUE}11. Nettoyage...${NC}"
rm -rf temp/

# ========================================
# RESUME FINAL
# ========================================
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}         CONSOLIDATION TERMINÉE${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""
echo -e "${GREEN}✓ Images totales : $TOTAL_IMAGES${NC}"
echo -e "${GREEN}✓ Annotations fusionnées : $TOTAL_ANNOTATIONS${NC}"
echo -e "${GREEN}✓ Nœuds OSM : $TOTAL_OSM_NODES${NC}"
echo -e "${GREEN}✓ Points SV : $TOTAL_SV_NODES${NC}"
echo -e "${GREEN}✓ Prédictions : $TOTAL_PREDICTIONS${NC}"
echo ""
echo "Fichiers créés dans :"
echo -e "${BLUE}gs://${BUCKET}/${DEST_PATH}/${NC}"
echo ""
echo "Structure :"
echo "  ├── annotations/"
echo "  │   └── annotations_all_wards.json"
echo "  ├── graphs/"
echo "  │   ├── osm_merged_all.json"
echo "  │   └── sv_merged_all.json"
echo "  ├── deliverables/"
echo "  │   └── all_wards.csv"
echo "  ├── stats/"
echo "  │   └── consolidation_stats.json"
echo "  └── consolidation_report.html"
echo ""
echo -e "${GREEN}✓ Consolidation complète !${NC}"