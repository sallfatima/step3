#!/bin/bash
# Fusionner les graphes OSM avec vérification

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/merged_data"

echo "=== Fusion des graphes OSM des 135 wards ==="

# Vérifier networkx
if ! python3 -c "import networkx" &>/dev/null; then
    echo "NetworkX n'est pas installé. Installation..."
    #pip3 install networkx
    pip3 install networkx pandas beautifulsoup4 openpyxl

fi

mkdir -p temp/osm_graphs

# Télécharger tous les graphes OSM disponibles
echo "Téléchargement des graphes OSM..."
count=0
for i in {1..135}; do
    echo -ne "\rRecherche ward $i/135..."
    osm_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/OSM_map_merged.json.json"
    if gcloud storage ls "$osm_file" &>/dev/null; then
        echo -ne "\rTéléchargement ward $i..."
        gcloud storage cp "$osm_file" "temp/osm_graphs/osm_ward_${i}.json" -q
        ((count++))
    fi
done
echo -e "\n$count graphes OSM trouvés"

if [ $count -eq 0 ]; then
    echo "ERREUR : Aucun graphe OSM trouvé !"
    echo "Vérifiez que les fichiers existent avec : ./check_data_availability.sh"
    exit 1
fi

# Fusionner avec Python
echo "Fusion des $count graphes..."
python3 << 'PYTHON_EOF'
import json
import sys

try:
    import networkx as nx
    from networkx.readwrite import json_graph
except ImportError:
    print("ERREUR : NetworkX n'est pas installé")
    print("Installez-le avec : pip3 install networkx")
    sys.exit(1)

import os

def merge_osm_graphs(input_dir, output_file):
    merged_graph = nx.Graph()
    
    files = sorted([f for f in os.listdir(input_dir) if f.endswith('.json')])
    if not files:
        print("Aucun fichier trouvé dans le dossier temp/osm_graphs")
        return
    
    total_nodes = 0
    total_edges = 0
    
    for idx, filename in enumerate(files):
        print(f"Traitement {idx+1}/{len(files)}: {filename}...")
        try:
            with open(os.path.join(input_dir, filename), 'r') as f:
                data = json.load(f)
                graph = json_graph.adjacency_graph(data)
                
                nodes_before = merged_graph.number_of_nodes()
                edges_before = merged_graph.number_of_edges()
                
                total_nodes += graph.number_of_nodes()
                total_edges += graph.number_of_edges()
                
                merged_graph = nx.compose(merged_graph, graph)
                
                new_nodes = merged_graph.number_of_nodes() - nodes_before
                new_edges = merged_graph.number_of_edges() - edges_before
                
                print(f"  - Nœuds ajoutés: {new_nodes}/{graph.number_of_nodes()}")
                
        except Exception as e:
            print(f"ERREUR lors du traitement de {filename}: {e}")
            continue
    
    if merged_graph.number_of_nodes() == 0:
        print("ERREUR : Le graphe fusionné est vide")
        return
    
    # Sauvegarder
    merged_data = json_graph.adjacency_data(merged_graph)
    with open(output_file, 'w') as f:
        json.dump(merged_data, f)
    
    print(f"\n=== Statistiques OSM ===")
    print(f"Fichiers traités : {len(files)}")
    print(f"Nœuds totaux avant fusion : {total_nodes}")
    print(f"Arêtes totales avant fusion : {total_edges}")
    print(f"Nœuds finaux (après dédoublonnage) : {merged_graph.number_of_nodes()}")
    print(f"Arêtes finales : {merged_graph.number_of_edges()}")
    print(f"Nœuds dédoublonnés : {total_nodes - merged_graph.number_of_nodes()}")
    
    # Sauvegarder les stats
    stats = {
        'files_processed': len(files),
        'total_nodes_before': total_nodes,
        'total_edges_before': total_edges,
        'final_nodes': merged_graph.number_of_nodes(),
        'final_edges': merged_graph.number_of_edges(),
        'deduplicated_nodes': total_nodes - merged_graph.number_of_nodes()
    }
    with open('temp/osm_merge_stats.json', 'w') as f:
        json.dump(stats, f, indent=2)

merge_osm_graphs('temp/osm_graphs', 'temp/osm_merged_global.json')
PYTHON_EOF

# Vérifier que le fichier a été créé
if [ -f "temp/osm_merged_global.json" ]; then
    # Upload
    gcloud storage cp "temp/osm_merged_global.json" "gs://${BUCKET}/${DEST_PATH}/osm_merged_all_wards.json"
    gcloud storage cp "temp/osm_merge_stats.json" "gs://${BUCKET}/${DEST_PATH}/osm_merge_stats.json"
    echo "✓ Graphe OSM fusionné : gs://${BUCKET}/${DEST_PATH}/osm_merged_all_wards.json"
else
    echo "ERREUR : Le fichier fusionné n'a pas été créé"
fi

# Nettoyer
rm -rf temp/osm_graphs