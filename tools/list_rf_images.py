import os, requests
from dotenv import load_dotenv, find_dotenv
import pandas as pd
# 1. Charge .env
load_dotenv(find_dotenv())

# 2. Variables
workspace = "lengo-geomapping"
project   = "geomapping_cpg_annotations"
api_key   = "ftEUZoJDm7N6nRICInwy"
if not api_key:
    raise RuntimeError("ROBOFLOW_KEY manquant dans .env")
# 2. Appel /jobs
url  = f"https://api.roboflow.com/{workspace}/{project}/jobs"
res  = requests.get(url, params={"api_key": api_key})
res.raise_for_status()
data = res.json()

jobs = data.get("jobs", [])

# 3. Filtrage et affichage
print(f"Nombre de jobs (avec >0 images) : {len([j for j in jobs if j.get('numImages', 0) > 0])}\n")

total_images = 0
for job in jobs:
    num_imgs = job.get("numImages", 0)
    if num_imgs == 0:
        continue  # on n’affiche pas les jobs à 0 image
    job_name = job.get("name") or job.get("id")
    print(f"- Job {job_name} : {num_imgs} images")
    total_images += num_imgs

print(f"\nTotal d’images (annotation_paths) : {total_images}")


jobs_data = res.json().get("jobs", [])
# 3. Filtrage et affichage
filtered_jobs = [job for job in jobs_data if job.get("numImages", 0) > 0]
print(f"Nombre de jobs (avec >0 images) : {len(filtered_jobs)}\n")

total_images = 0
for job in filtered_jobs:
    num_imgs = job.get("numImages", 0)
    job_name = job.get("name") or job.get("id")
    print(f"- Job {job_name} : {num_imgs} images")
    total_images += num_imgs

print(f"\nTotal d’images (annotation_paths) : {total_images}\n")

# 4. Préparation des données triées
jobs = [
    {"job_name": job.get("name") or job.get("id"), "num_images": job.get("numImages", 0)}
    for job in filtered_jobs
]
jobs_sorted = sorted(jobs, key=lambda x: x["job_name"])

# 5. Création du DataFrame et export vers Excel
df = pd.DataFrame(jobs_sorted)
excel_path = "rf_jobs_sorted.xlsx"
df.to_excel(excel_path, index=False)

# 7. Afficher le chemin du fichier Excel
print(f"Fichier Excel exporté vers : {excel_path}")