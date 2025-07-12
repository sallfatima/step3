#!/bin/bash

# Pour tous les wards de 1 à 135
for i in {1..135}; do
    echo "Copie du polygon pour ward $i..."
    
    source_file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/polygon_custom.geojson"
    dest_file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/polygon_custom${i}.geojson"
    
    if gcloud storage ls "$source_file" &>/dev/null; then
        gcloud storage cp "$source_file" "$dest_file"
        echo "✓ Ward $i : polygon copié avec succès"
    else
        echo "✗ Ward $i : polygon_custom.geojson non trouvé"
    fi
done

echo "Terminé !"