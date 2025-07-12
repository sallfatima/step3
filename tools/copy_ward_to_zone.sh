#!/bin/bash

# Boucle de 1 à 135
for i in {1..135}; do
    echo "Traitement du ward $i..."
    
    # Définir les chemins source et destination
    source_file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/annotations/annotations_ward_${i}_no_duplicates_image_location.json" 
    dest_file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/annotations/annotations_no_duplicates_image_location.json"
  

    # Vérifier si le fichier source existe
    if gcloud storage ls "$source_file" &>/dev/null; then
        # Copier le fichier
        gcloud storage cp "$source_file" "$dest_file"
        echo "✓ Ward $i : copié avec succès"
    else
        echo "✗ Ward $i : fichier annotations_ward_${i}_no_duplicates_image_location.json non trouvé"
    fi
done


echo "Terminé !"

