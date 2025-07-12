#!/bin/bash

echo "Suppression des fichiers pour les wards 1 à 135..."
echo "=================================================="

for i in {1..135}; do
    echo "Ward $i..."
    
    # Fichiers/dossiers à supprimer
    files_to_delete=(
        # "gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/annotations/annotations_zone_${i}.json"
        # "gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/annotations/annotations_zone_${i}_no_duplicates_image.json"
        # "gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/output"
        # "gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/polygon_zone_050.geojson"
        # "gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/polygon_zone_${i}.geojson"

        #"gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/polygon_ward_${i}.geojson"
             
    )
    
    # Supprimer chaque fichier/dossier
    for file in "${files_to_delete[@]}"; do
        if gcloud storage ls "$file" &>/dev/null; then
            # Pour les dossiers, utiliser -r (récursif)
            if [[ "$file" == *"/output" ]]; then
                gcloud storage rm -r "$file"
                echo "  ✓ Supprimé : output/"
            else
                gcloud storage rm "$file"
                echo "  ✓ Supprimé : $(basename $file)"
            fi
        else
            echo "  - Non trouvé : $(basename $file)"
        fi
    done
done

echo "=================================================="
echo "Suppression terminée !"