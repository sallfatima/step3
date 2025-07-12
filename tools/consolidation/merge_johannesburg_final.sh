#!/bin/bash

# ========================================
# SCRIPT DE FUSION COMPLET JOHANNESBURG
# ========================================

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/johannesburg_custom_all"
TOTAL_WARDS=135

# Couleurs
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}   FUSION COMPLETE JOHANNESBURG${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""
echo "Fichiers à fusionner :"
echo "  ✓ OSM_map_merged.json"
echo "  ✓ SV_map_merged.json"
echo "  ✓ Annotations"
echo "  ✓ card_main.html (stats)"
echo "  ✓ Deliverables"
echo ""
read -p "Continuer ? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 0
fi

# Créer les dossiers temporaires
mkdir -p temp/{osm,sv,annotations,cards,stats}

# Vérifier jq
if ! command -v jq &> /dev/null; then
    echo "Installation de jq..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install jq
    else
        sudo apt-get update && sudo apt-get install -y jq
    fi
fi

# ========================================
# 1. FUSIONNER OSM_map_merged.json
# ========================================
echo -e "\n${BLUE}1. Fusion des fichiers OSM_map_merged.json...${NC}"

osm_count=0
# Initialiser avec la structure correcte
echo '{"directed": false, "multigraph": false, "graph": [], "nodes": [], "links": []}' > temp/osm_merged.json

for i in {1..135}; do
    echo -ne "\rTraitement ward $i/135..."
    
    osm_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/OSM_map_merged.json"
    
    if gcloud storage ls "$osm_file" &>/dev/null; then
        # Télécharger
        gcloud storage cp "$osm_file" "temp/osm/osm_${i}.json" -q
        
        # Fusionner les nœuds et liens
        jq -s '
            .[0] as $merged | 
            .[1] as $new | 
            {
                directed: false,
                multigraph: false,
                graph: [],
                nodes: ($merged.nodes + $new.nodes),
                links: (($merged.links // []) + ($new.links // []))
            }
        ' temp/osm_merged.json "temp/osm/osm_${i}.json" > temp/osm_tmp.json
        
        mv temp/osm_tmp.json temp/osm_merged.json
        rm "temp/osm/osm_${i}.json"
        ((osm_count++))
    fi
done

echo -e "\n$osm_count fichiers OSM trouvés"

if [ $osm_count -gt 0 ]; then
    # Dédoublonner les nœuds par ID
    echo "Dédoublonnage des nœuds OSM..."
    jq '.nodes |= unique_by(.id)' temp/osm_merged.json > temp/osm_dedup.json
    
    nodes_count=$(jq '.nodes | length' temp/osm_dedup.json)
    links_count=$(jq '.links | length // 0' temp/osm_dedup.json)
    
    echo -e "${GREEN}✓ OSM : $nodes_count nœuds uniques, $links_count liens${NC}"
    
    # Upload
    gcloud storage cp temp/osm_dedup.json "gs://${BUCKET}/${DEST_PATH}/OSM_map_merged_all.json"
fi

# ========================================
# 2. FUSIONNER SV_map_merged.json
# ========================================
echo -e "\n${BLUE}2. Fusion des fichiers SV_map_merged.json...${NC}"

sv_count=0
echo '{"directed": false, "multigraph": false, "graph": [], "nodes": [], "links": []}' > temp/sv_merged.json

for i in {1..135}; do
    echo -ne "\rTraitement ward $i/135..."
    
    sv_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/SV_map_merged.json"
    
    if gcloud storage ls "$sv_file" &>/dev/null; then
        gcloud storage cp "$sv_file" "temp/sv/sv_${i}.json" -q
        
        jq -s '
            .[0] as $merged | 
            .[1] as $new | 
            {
                directed: false,
                multigraph: false,
                graph: [],
                nodes: ($merged.nodes + $new.nodes),
                links: (($merged.links // []) + ($new.links // []))
            }
        ' temp/sv_merged.json "temp/sv/sv_${i}.json" > temp/sv_tmp.json
        
        mv temp/sv_tmp.json temp/sv_merged.json
        rm "temp/sv/sv_${i}.json"
        ((sv_count++))
    fi
done

echo -e "\n$sv_count fichiers SV trouvés"

if [ $sv_count -gt 0 ]; then
    # Dédoublonner
    echo "Dédoublonnage des points SV..."
    jq '.nodes |= unique_by(.id)' temp/sv_merged.json > temp/sv_dedup.json
    
    nodes_count=$(jq '.nodes | length' temp/sv_dedup.json)
    links_count=$(jq '.links | length // 0' temp/sv_dedup.json)
    
    echo -e "${GREEN}✓ SV : $nodes_count points uniques, $links_count liens${NC}"
    
    # Upload
    gcloud storage cp temp/sv_dedup.json "gs://${BUCKET}/${DEST_PATH}/SV_map_merged_all.json"
fi

# ========================================
# 3. FUSIONNER LES ANNOTATIONS
# ========================================
echo -e "\n${BLUE}3. Fusion des annotations...${NC}"

annotation_count=0
# Initialiser le fichier de fusion
echo '{"images": [], "annotations": [], "categories": []}' > temp/annotations_merged.json

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
    
    # Traiter chaque fichier
    for file in temp/annotations/*.json; do
        if [ -f "$file" ]; then
            # Fusionner en préservant les catégories du premier fichier
            jq -s '
                .[0] as $merged | 
                .[1] as $new | 
                {
                    images: ($merged.images + $new.images),
                    annotations: ($merged.annotations + $new.annotations),
                    categories: (if ($merged.categories | length) == 0 then $new.categories else $merged.categories end)
                }
            ' temp/annotations_merged.json "$file" > temp/annotations_tmp.json
            
            mv temp/annotations_tmp.json temp/annotations_merged.json
        fi
    done
    
    # Compter
    images_count=$(jq '.images | length' temp/annotations_merged.json)
    annotations_count=$(jq '.annotations | length' temp/annotations_merged.json)
    
    echo -e "${GREEN}✓ Annotations : $images_count images, $annotations_count annotations${NC}"
    
    # Upload
    gcloud storage cp temp/annotations_merged.json "gs://${BUCKET}/${DEST_PATH}/annotations_merged_all.json"
fi

# ========================================
# 4. COLLECTER LES STATISTIQUES DES CARTES HTML
# ========================================
echo -e "\n${BLUE}4. Collecte des cartes HTML...${NC}"

card_count=0
osm_html_count=0
sv_html_count=0

# Créer un index HTML
cat > temp/index_cards.html << 'HTML_EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Johannesburg - Index des Cartes</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        .ward-list { 
            display: grid; 
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); 
            gap: 10px; 
        }
        .ward-item { 
            padding: 10px; 
            border: 1px solid #ddd; 
            border-radius: 5px; 
        }
        .available { background-color: #e8f5e9; }
        .missing { background-color: #ffebee; }
    </style>
</head>
<body>
    <h1>Johannesburg - Cartes par Ward</h1>
    <div class="ward-list">
HTML_EOF

for i in {1..135}; do
    echo -ne "\rAnalyse ward $i/135..."
    
    card_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/card_main.html"
    osm_html="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/OSM_map_merged.html"
    sv_html="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/SV_map_merged.html"
    
    has_files=false
    
    # Vérifier et copier les fichiers HTML
    if gcloud storage ls "$card_file" &>/dev/null; then
        gcloud storage cp "$card_file" "gs://${BUCKET}/${DEST_PATH}/cards/ward_${i}_card_main.html" -q
        ((card_count++))
        has_files=true
    fi
    
    if gcloud storage ls "$osm_html" &>/dev/null; then
        gcloud storage cp "$osm_html" "gs://${BUCKET}/${DEST_PATH}/maps/ward_${i}_OSM_map.html" -q
        ((osm_html_count++))
        has_files=true
    fi
    
    if gcloud storage ls "$sv_html" &>/dev/null; then
        gcloud storage cp "$sv_html" "gs://${BUCKET}/${DEST_PATH}/maps/ward_${i}_SV_map.html" -q
        ((sv_html_count++))
        has_files=true
    fi
    
    # Ajouter à l'index
    if [ "$has_files" = true ]; then
        echo "<div class='ward-item available'>Ward $i ✓</div>" >> temp/index_cards.html
    else
        echo "<div class='ward-item missing'>Ward $i ✗</div>" >> temp/index_cards.html
    fi
done

# Fermer l'index HTML
cat >> temp/index_cards.html << 'HTML_EOF'
    </div>
    <hr>
    <p>Total cartes : CARD_COUNT/135</p>
    <p>Total OSM HTML : OSM_HTML_COUNT/135</p>
    <p>Total SV HTML : SV_HTML_COUNT/135</p>
</body>
</html>
HTML_EOF

# Remplacer les compteurs
sed -i "s/CARD_COUNT/$card_count/g" temp/index_cards.html
sed -i "s/OSM_HTML_COUNT/$osm_html_count/g" temp/index_cards.html
sed -i "s/SV_HTML_COUNT/$sv_html_count/g" temp/index_cards.html

# Upload l'index
gcloud storage cp temp/index_cards.html "gs://${BUCKET}/${DEST_PATH}/index_cards.html"

echo -e "\n${GREEN}✓ HTML : $card_count cartes, $osm_html_count OSM, $sv_html_count SV${NC}"

# ========================================
# 5. FUSIONNER LES FICHIERS EXCEL
# ========================================
echo -e "\n${BLUE}5. Fusion des fichiers Excel...${NC}"

excel_count=0
mkdir -p temp/excel

# Télécharger tous les fichiers Excel
for i in {1..135}; do
    echo -ne "\rRecherche Excel ward $i/135..."
    
    excel_files=$(gcloud storage ls "gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/deliverables/*.xlsx" 2>/dev/null || true)
    
    for file in $excel_files; do
        if [ ! -z "$file" ]; then
            filename=$(basename "$file")
            gcloud storage cp "$file" "temp/excel/ward_${i}_${filename}" -q
            ((excel_count++))
        fi
    done
done

echo -e "\n${GREEN}✓ Fichiers Excel trouvés : $excel_count${NC}"

# Si on a pandas, fusionner les Excel
if [ $excel_count -gt 0 ]; then
    if python3 -c "import pandas" &>/dev/null; then
        echo "Fusion des fichiers Excel avec pandas..."
        
        python3 << 'PYTHON_EOF'
import pandas as pd
import os
import glob

excel_files = glob.glob('temp/excel/*.xlsx')
all_data = []

for file in excel_files:
    try:
        df = pd.read_excel(file)
        ward = os.path.basename(file).split('_')[1]
        df['ward'] = ward
        all_data.append(df)
    except:
        pass

if all_data:
    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df.to_excel('temp/deliverables_merged.xlsx', index=False)
    print(f"Excel fusionné : {len(merged_df)} lignes")
PYTHON_EOF
        
        if [ -f "temp/deliverables_merged.xlsx" ]; then
            gcloud storage cp "temp/deliverables_merged.xlsx" "gs://${BUCKET}/${DEST_PATH}/deliverables_merged.xlsx"
        fi
    else
        echo "Pandas non disponible - copie simple des Excel"
        # Juste copier tous les Excel dans un dossier
        for file in temp/excel/*.xlsx; do
            if [ -f "$file" ]; then
                gcloud storage cp "$file" "gs://${BUCKET}/${DEST_PATH}/excel/" -q
            fi
        done
    fi
fi

# ========================================
# 6. FUSIONNER LES CSV
# ========================================
echo -e "\n${BLUE}6. Fusion des fichiers CSV...${NC}"

csv_count=0
first_file=true

# Créer le fichier de sortie
> temp/deliverables_merged.csv

for i in {1..135}; do
    echo -ne "\rRecherche ward $i/135..."
    
    # Chercher les CSV
    csv_files=$(gcloud storage ls "gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/deliverables/*.csv" 2>/dev/null || true)
    
    for file in $csv_files; do
        if [ ! -z "$file" ]; then
            # Télécharger temporairement
            gcloud storage cp "$file" "temp/temp_ward.csv" -q
            
            if [ "$first_file" = true ]; then
                # Copier avec l'en-tête
                cat "temp/temp_ward.csv" > temp/deliverables_merged.csv
                first_file=false
            else
                # Copier sans l'en-tête
                tail -n +2 "temp/temp_ward.csv" >> temp/deliverables_merged.csv
            fi
            
            rm "temp/temp_ward.csv"
            ((csv_count++))
        fi
    done
done

echo -e "\n${GREEN}✓ Fichiers CSV fusionnés : $csv_count${NC}"

if [ $csv_count -gt 0 ]; then
    gcloud storage cp temp/deliverables_merged.csv "gs://${BUCKET}/${DEST_PATH}/deliverables_merged.csv"
fi

# ========================================
# 7. GÉNÉRER LE RAPPORT FINAL
# ========================================
echo -e "\n${BLUE}7. Génération du rapport...${NC}"

# Créer le rapport JSON
cat > temp/consolidation_report.json << EOF
{
    "consolidation_date": "$(date -u +%Y-%m-%d\ %H:%M:%S) UTC",
    "total_wards": $TOTAL_WARDS,
    "files_processed": {
        "osm_json": $osm_count,
        "sv_json": $sv_count,
        "annotation_files": $annotation_count,
        "card_html": $card_count,
        "osm_html": $osm_html_count,
        "sv_html": $sv_html_count,
        "excel_files": $excel_count,
        "csv_files": $csv_count
    },
    "statistics": {
        "osm_nodes": $([ -f temp/osm_dedup.json ] && jq '.nodes | length' temp/osm_dedup.json || echo 0),
        "sv_nodes": $([ -f temp/sv_dedup.json ] && jq '.nodes | length' temp/sv_dedup.json || echo 0),
        "total_images": $([ -f temp/annotations_merged.json ] && jq '.images | length' temp/annotations_merged.json || echo 0),
        "total_annotations": $([ -f temp/annotations_merged.json ] && jq '.annotations | length' temp/annotations_merged.json || echo 0)
    },
    "output_files": [
        "OSM_map_merged_all.json",
        "SV_map_merged_all.json",
        "annotations_merged_all.json",
        "deliverables_merged.xlsx",
        "deliverables_merged.csv",
        "index_cards.html",
        "cards/ward_*_card_main.html",
        "maps/ward_*_OSM_map.html",
        "maps/ward_*_SV_map.html"
    ]
}
EOF

# Upload le rapport
gcloud storage cp temp/consolidation_report.json "gs://${BUCKET}/${DEST_PATH}/consolidation_report.json"

# ========================================
# 8. NETTOYAGE
# ========================================
echo -e "\n${BLUE}8. Nettoyage...${NC}"
rm -rf temp/

# ========================================
# RÉSUMÉ FINAL
# ========================================
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}       FUSION TERMINÉE !${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""
echo -e "${GREEN}Fichiers traités :${NC}"
echo "  • OSM JSON : $osm_count/135"
echo "  • SV JSON : $sv_count/135"
echo "  • Annotations : $annotation_count/135"
echo "  • Cartes HTML : $card_count/135"
echo "  • OSM HTML : $osm_html_count/135"
echo "  • SV HTML : $sv_html_count/135"
echo "  • Excel : $excel_count fichiers"
echo "  • CSV : $csv_count fichiers"
echo ""
echo -e "${GREEN}Fichiers créés dans :${NC}"
echo -e "${BLUE}gs://${BUCKET}/${DEST_PATH}/${NC}"
echo ""
echo "  ├── OSM_map_merged_all.json"
echo "  ├── SV_map_merged_all.json"
echo "  ├── annotations_merged_all.json"
echo "  ├── deliverables_merged.xlsx"
echo "  ├── deliverables_merged.csv"
echo "  ├── index_cards.html"
echo "  ├── consolidation_report.json"
echo "  ├── cards/"
echo "  │   └── ward_*_card_main.html"
echo "  └── maps/"
echo "      ├── ward_*_OSM_map.html"
echo "      └── ward_*_SV_map.html"
echo ""
echo -e "${GREEN}✓ Consolidation complète !${NC}"

