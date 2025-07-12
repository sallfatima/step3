#!/bin/bash
# Fusionner tous les graphes Street View

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/merged_data"

echo "=== Fusion des graphes Street View des 135 wards ==="

mkdir -p temp/sv_graphs

# Télécharger
echo "Téléchargement des graphes SV..."
count=0
for i in {1..135}; do
    echo -ne "\rWard $i/135..."
    sv_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/data/SV_map_merged.json"
    if gcloud storage ls "$sv_file" &>/dev/null; then
        gcloud storage cp "$sv_file" "temp/sv_graphs/sv_ward_${i}.json" -q
        ((count++))
    fi
done
echo -e "\n$count graphes SV trouvés"

# Fusionner (même logique que OSM)
echo "Fusion des graphes..."
python3 << 'PYTHON_EOF'
import json
import networkx as nx
from networkx.readwrite import json_graph
import os

def merge_sv_graphs(input_dir, output_file):
    merged_graph = nx.Graph()
    
    files = sorted([f for f in os.listdir(input_dir) if f.endswith('.json')])
    total_nodes = 0
    total_edges = 0
    
    for filename in files:
        print(f"Processing {filename}...")
        with open(os.path.join(input_dir, filename), 'r') as f:
            data = json.load(f)
            graph = json_graph.adjacency_graph(data)
            
            total_nodes += graph.number_of_nodes()
            total_edges += graph.number_of_edges()
            
            merged_graph = nx.compose(merged_graph, graph)
    
    # Sauvegarder
    merged_data = json_graph.adjacency_data(merged_graph)
    with open(output_file, 'w') as f:
        json.dump(merged_data, f)
    
    print(f"\n=== Statistiques Street View ===")
    print(f"Graphes fusionnés : {len(files)}")
    print(f"Nœuds totaux avant dédoublonnage : {total_nodes}")
    print(f"Nœuds finaux (uniques) : {merged_graph.number_of_nodes()}")
    print(f"Points SV dédoublonnés : {total_nodes - merged_graph.number_of_nodes()}")

merge_sv_graphs('temp/sv_graphs', 'temp/sv_merged_global.json')
PYTHON_EOF

# Upload
gcloud storage cp "temp/sv_merged_global.json" "gs://${BUCKET}/${DEST_PATH}/sv_merged_all_wards.json"
echo "✓ Graphe SV fusionné : gs://${BUCKET}/${DEST_PATH}/sv_merged_all_wards.json"

rm -rf temp/sv_graphs