#!/bin/bash

# Compteurs
found=0
missing=0
missing_wards=()

echo "Vérification des fichiers annotations_ward_X.json pour les wards 1 à 135..."
echo "================================================================"

for i in {1..135}; do


    #file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/polygon_custom_ward_${i}.json"
    #file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/annotations/annotations_ward_${i}.json"
    #file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/annotations/annotations_ward_${i}_no_duplicates_image.json"

    file="gs://lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_${i}/annotations/annotations_ward_${i}_no_duplicates_image_location.json"

   # https://storage.cloud.google.com/lengo-geomapping/database/africa/south_africa/johannesburg/johannesburg_custom_ward_28/annotations/annotations_ward_28_no_duplicates_image_location.json
    
    if gcloud storage ls "$file" &>/dev/null; then
        echo "✓ Ward $i : EXISTE"
        ((found++))
    else
        echo "✗ Ward $i : MANQUANT"
        ((missing++))
        missing_wards+=($i)
    fi
done

echo "================================================================"
echo "RÉSUMÉ :"
echo "- Fichiers trouvés : $found"
echo "- Fichiers manquants : $missing"

if [ $missing -gt 0 ]; then
    echo ""
    echo "Wards avec fichiers manquants : ${missing_wards[@]}"
fi