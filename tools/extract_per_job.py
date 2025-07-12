import os
import json
import shutil
import requests
from roboflow import Roboflow

def quick_job_extractor(target_job_names):
    """
    Solution RAPIDE pour extraire des jobs spécifiques
    Usage: quick_job_extractor(["abuja_custom20", "abuja_custom7"])
    """
    
    # Configuration
    api_key = "ftEUZoJDm7N6nRICInwy"
    workspace = "lengo-geomapping"
    project_name = "geomapping_cpg_annotations"
    
    print(f"🎯 Extraction de {len(target_job_names)} jobs spécifiques")
    print(f"Jobs demandés: {target_job_names}")
    
    # 1. Télécharger le dataset complet (une seule fois)
    rf = Roboflow(api_key=api_key)
    project = rf.workspace(workspace).project(project_name)
    

    version = project.version(15)
 

    
    print(f"\n📥 Téléchargement du dataset complet...")
    dataset = version.download("coco", location="./temp_complete_dataset")
    print(f"✅ Dataset téléchargé: {dataset.location}")
    
    # 2. Créer les dossiers pour chaque job demandé
    output_base = "./extracted_jobs"
    os.makedirs(output_base, exist_ok=True)
    
    results = {}
    
    for job_name in target_job_names:
        print(f"\n📁 Extraction de: {job_name}")
        
        job_output = os.path.join(output_base, job_name)
        os.makedirs(job_output, exist_ok=True)
        
        total_extracted = 0
        
        # 3. Parcourir chaque split (train/valid/test)
        for split in ['train', 'valid', 'test']:
            source_split_dir = os.path.join(dataset.location, split)
            target_split_dir = os.path.join(job_output, split)
            
            if not os.path.exists(source_split_dir):
                continue
                
            os.makedirs(target_split_dir, exist_ok=True)
            
            # Lire les annotations
            annotation_file = os.path.join(source_split_dir, '_annotations.coco.json')
            if not os.path.exists(annotation_file):
                continue
            
            with open(annotation_file, 'r') as f:
                coco_data = json.load(f)
            
            # Filtrer les images pour ce job
            job_images = []
            job_annotations = []
            
            for image in coco_data.get('images', []):
                file_name = image.get('file_name', '')
                
                # Logique de matching - ajustez selon vos patterns
                if (job_name in file_name or 
                    file_name.startswith(job_name) or
                    f"/{job_name}/" in file_name):
                    
                    job_images.append(image)
                    
                    # Copier l'image
                    source_img = os.path.join(source_split_dir, file_name)
                    target_img = os.path.join(target_split_dir, file_name)
                    
                    if os.path.exists(source_img):
                        shutil.copy2(source_img, target_img)
                        total_extracted += 1
                    
                    # Récupérer les annotations
                    image_id = image.get('id')
                    for ann in coco_data.get('annotations', []):
                        if ann.get('image_id') == image_id:
                            job_annotations.append(ann)
            
            # Sauvegarder les annotations filtrées
            if job_images:
                filtered_coco = {
                    'images': job_images,
                    'annotations': job_annotations,
                    'categories': coco_data.get('categories', [])
                }
                
                target_annotation = os.path.join(target_split_dir, '_annotations.coco.json')
                with open(target_annotation, 'w') as f:
                    json.dump(filtered_coco, f, indent=2)
                
                print(f"   {split}: {len(job_images)} images")
        
        results[job_name] = total_extracted
        print(f"   ✅ Total: {total_extracted} images extraites")
    
    # 4. Nettoyer le dataset temporaire
    print(f"\n🧹 Nettoyage...")
    shutil.rmtree(dataset.location)
    
    # 5. Résumé
    print(f"\n📊 RÉSUMÉ:")
    for job_name, count in results.items():
        print(f"   {job_name}: {count} images")
    
    print(f"\n📁 Jobs extraits dans: {output_base}")
    return results

def smart_pattern_matching(job_name, file_name):
    """
    Logique intelligente pour matcher les noms de jobs avec les noms de fichiers
    Ajustez cette fonction selon vos patterns spécifiques
    """
    
    # Patterns possibles basés sur votre liste
    patterns = [
        job_name,  # Match exact
        job_name.replace('_', '-'),  # Avec tirets
        job_name.split('_')[-1],  # Juste le numéro (ex: custom20)
        f"/{job_name}/",  # Dans le path
        f"_{job_name}_",  # Entouré d'underscores
    ]
    
    for pattern in patterns:
        if pattern in file_name.lower():
            return True
    
    return False

def batch_extract_all_jobs():
    """
    Extraire TOUS les jobs automatiquement
    """
    
    # Configuration
    api_key = "ftEUZoJDm7N6nRICInwy"
    workspace = "lengo-geomapping"
    project_name = "geomapping_cpg_annotations"
    
    print("🔄 Extraction de TOUS les jobs automatiquement")
    
    # 1. Récupérer la liste complète des jobs
    jobs_url = f"https://api.roboflow.com/{workspace}/{project_name}/jobs"
    res = requests.get(jobs_url, params={"api_key": api_key})
    jobs = [job for job in res.json().get("jobs", []) if job.get("numImages", 0) > 0]
    
    # Extraire les noms de jobs uniques
    job_names = []
    for job in jobs:
        job_name = job.get('name', '').replace('Code upload - ', '')
        if 'africa/nigeria/abuja/' in job_name:
            clean_name = job_name.split('/')[-1]  # Prendre juste la partie finale
            if clean_name not in job_names:
                job_names.append(clean_name)
    
    print(f"📋 {len(job_names)} jobs uniques trouvés")
    print(f"Premiers jobs: {job_names[:10]}")
    
    # 2. Extraire par batches (pour éviter la surcharge mémoire)
    batch_size = 10
    for i in range(0, len(job_names), batch_size):
        batch = job_names[i:i+batch_size]
        print(f"\n🔄 Traitement du batch {i//batch_size + 1}: {batch}")
        
        results = quick_job_extractor(batch)
        
        print(f"✅ Batch {i//batch_size + 1} terminé")

# Exemple d'utilisation
if __name__ == "__main__":
    
    # OPTION 1: Extraire des jobs spécifiques
    print("=" * 60)
    print("OPTION 1: Jobs spécifiques")
    print("=" * 60)
    
    # Modifiez cette liste avec vos jobs souhaités
    my_target_jobs = [
        "abuja_custom20",
        "abuja_custom7", 
        "abuja_custom3",
        "abuja_custom17"
    ]
    
    results = quick_job_extractor(my_target_jobs)
    
    # OPTION 2: Extraire tous les jobs (décommentez si nécessaire)
    # print("\n" + "=" * 60)
    # print("OPTION 2: Tous les jobs")
    # print("=" * 60)
    # batch_extract_all_jobs()
    
    print(f"\n✅ TERMINÉ!")
    print(f"Vos datasets sont dans le dossier './extracted_jobs/'")