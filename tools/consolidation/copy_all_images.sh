#!/bin/bash
# Copier toutes les images des 135 wards vers un dossier global

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/johannesburg_custom_all/images"

echo "Copie de toutes les images vers images_global..."

# Boucle sur les 135 wards
for i in {1..135}; do
    echo "Ward $i..."
    SOURCE="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/images/*.jpg"
    DEST="gs://${BUCKET}/${DEST_PATH}/"
    
    # Copier les images
    gcloud storage cp ${SOURCE} ${DEST} 2>/dev/null || echo "  Pas d'images dans ward $i"
done

echo "✓ Copie terminée !"
# Compter le total
TOTAL=$(gcloud storage ls "gs://${BUCKET}/${DEST_PATH}/*.jpg" | wc -l)
echo "Total images copiées : $TOTAL"
EOF