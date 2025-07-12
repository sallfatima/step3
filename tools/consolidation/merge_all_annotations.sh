#!/bin/bash
# Fusionner toutes les annotations des 135 wards en un seul fichier

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/johannesburg_custom_all"

echo "=== Fusion des annotations des 135 wards ==="

# Créer un dossier temporaire
mkdir -p temp/annotations

# Télécharger toutes les annotations
echo "Téléchargement des annotations..."
for i in {1..135}; do
    echo -ne "\rWard $i/135..."
    
    # Priorité aux fichiers sans doublons
    ann_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/annotations/annotations_no_duplicates_image_location.json"
    alt_file="gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/annotations/annotations.json"
    
    if gcloud storage ls "$ann_file" &>/dev/null; then
        gcloud storage cp "$ann_file" "temp/annotations/ward_${i}.json" -q
    elif gcloud storage ls "$alt_file" &>/dev/null; then
        gcloud storage cp "$alt_file" "temp/annotations/ward_${i}.json" -q
    fi
done

echo -e "\n\nFusion des annotations..."

# Script Python pour fusionner
python3 << 'PYTHON_EOF'
import json
import os
from collections import defaultdict

def merge_coco_annotations(input_dir, output_file):
    merged = {
        "images": [],
        "annotations": [],
        "categories": []
    }
    
    image_id_offset = 0
    annotation_id_offset = 0
    categories_added = False
    
    # Stats
    total_images = 0
    total_annotations = 0
    images_per_ward = {}
    
    files = sorted([f for f in os.listdir(input_dir) if f.endswith('.json')])
    
    for file_idx, filename in enumerate(files):
        ward_num = filename.replace('ward_', '').replace('.json', '')
        print(f"Processing ward {ward_num}...")
        
        with open(os.path.join(input_dir, filename), 'r') as f:
            data = json.load(f)
        
        # Ajouter les catégories une seule fois
        if not categories_added and 'categories' in data:
            merged['categories'] = data['categories']
            categories_added = True
        
        # Mapper les anciens IDs vers les nouveaux
        image_id_map = {}
        
        # Ajouter les images avec nouveaux IDs
        for img in data.get('images', []):
            old_id = img['id']
            new_id = old_id + image_id_offset
            image_id_map[old_id] = new_id
            img['id'] = new_id
            merged['images'].append(img)
        
        # Ajouter les annotations avec nouveaux IDs
        for ann in data.get('annotations', []):
            ann['id'] = ann['id'] + annotation_id_offset
            ann['image_id'] = image_id_map[ann['image_id']]
            merged['annotations'].append(ann)
        
        # Stats
        num_images = len(data.get('images', []))
        num_annotations = len(data.get('annotations', []))
        images_per_ward[ward_num] = num_images
        total_images += num_images
        total_annotations += num_annotations
        
        # Mettre à jour les offsets
        if data.get('images'):
            image_id_offset = max([img['id'] for img in merged['images']]) + 1
        if data.get('annotations'):
            annotation_id_offset = max([ann['id'] for ann in merged['annotations']]) + 1
    
    # Sauvegarder
    with open(output_file, 'w') as f:
        json.dump(merged, f, indent=2)
    
    # Afficher les stats
    print(f"\n=== Statistiques de fusion ===")
    print(f"Wards traités : {len(files)}")
    print(f"Total images : {total_images}")
    print(f"Total annotations : {total_annotations}")
    print(f"Moyenne images/ward : {total_images/len(files):.1f}")
    print(f"Moyenne annotations/ward : {total_annotations/len(files):.1f}")
    
    # Sauvegarder les stats
    stats = {
        "total_wards": len(files),
        "total_images": total_images,
        "total_annotations": total_annotations,
        "images_per_ward": images_per_ward
    }
    with open('temp/merge_stats.json', 'w') as f:
        json.dump(stats, f, indent=2)

merge_coco_annotations('temp/annotations', 'temp/merged_annotations.json')
PYTHON_EOF

# Upload
echo -e "\nUpload vers GCS..."
gcloud storage cp "temp/merged_annotations.json" "gs://${BUCKET}/${DEST_PATH}/annotations_merged_all_wards.json"
gcloud storage cp "temp/merge_stats.json" "gs://${BUCKET}/${DEST_PATH}/annotations_merge_stats.json"

echo "✓ Annotations fusionnées : gs://${BUCKET}/${DEST_PATH}/annotations_merged_all_wards.json"

# Nettoyer
rm -rf temp/annotations