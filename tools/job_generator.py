import json
import os
from typing import Dict, List, Optional

def load_ward_coordinates_from_geojson(file_path: str = "johannesburg/johannesburg_inv.geojson") -> Dict[str, List[List[float]]]:
    """
    Charge les coordonnées des wards depuis le fichier GeoJSON
    
    Args:
        file_path: Chemin vers le fichier GeoJSON
        
    Returns:
        Dictionnaire avec les IDs des wards et leurs coordonnées de polygone
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        ward_coordinates = {}
        
        # Parcourir les features du GeoJSON
        for feature in geojson_data.get('features', []):
            # Extraire les propriétés du ward
            properties = feature.get('properties', {})
            ward_id = properties.get('WardID')
            ward_no = properties.get('WardNo')
            
            # Utiliser WardID si disponible, sinon construire à partir de WardNo
            if ward_id:
                key = ward_id
            elif ward_no:
                key = f"7980{ward_no:04d}"  # Format: 79800001, 79800002, etc.
            else:
                print(f"⚠️ Feature sans WardID ou WardNo ignorée")
                continue
            
            # Extraire la géométrie
            geometry = feature.get('geometry', {})
            if geometry.get('type') == 'Polygon':
                # Pour un polygone, prendre les coordonnées du contour externe
                coordinates = geometry.get('coordinates', [])[0]  # Premier contour (externe)
                ward_coordinates[key] = coordinates
                
            elif geometry.get('type') == 'MultiPolygon':
                # Pour un multipolygone, prendre le premier polygone
                coordinates = geometry.get('coordinates', [])[0][0]  # Premier polygone, premier contour
                ward_coordinates[key] = coordinates
            else:
                print(f"⚠️ Type de géométrie non supporté pour le ward {key}: {geometry.get('type')}")
                continue
        
        print(f"✅ {len(ward_coordinates)} wards chargés depuis {file_path}")
        return ward_coordinates
        
    except FileNotFoundError:
        print(f"❌ Fichier {file_path} non trouvé")
        return {}
    except json.JSONDecodeError:
        print(f"❌ Erreur de format JSON dans {file_path}")
        return {}
    except Exception as e:
        print(f"❌ Erreur lors du chargement du fichier: {e}")
        return {}

def extract_ward_info_from_geojson(file_path: str = "johannesburg/johannesburg_inv.geojson") -> None:
    """
    Affiche des informations sur les wards contenus dans le GeoJSON pour debug
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print(f"📊 ANALYSE DU FICHIER GEOJSON: {file_path}")
        print("=" * 60)
        
        features = geojson_data.get('features', [])
        print(f"Nombre total de features: {len(features)}")
        
        # Analyser les premières features
        for i, feature in enumerate(features[:5]):
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            print(f"\n🔍 Feature {i+1}:")
            print(f"   Type géométrie: {geometry.get('type', 'Non défini')}")
            print(f"   WardID: {properties.get('WardID', 'Non défini')}")
            print(f"   WardNo: {properties.get('WardNo', 'Non défini')}")
            print(f"   WardLabel: {properties.get('WardLabel', 'Non défini')}")
            print(f"   Municipality: {properties.get('Municipali', 'Non défini')}")
            
            # Compter les coordonnées
            coords = geometry.get('coordinates', [])
            if geometry.get('type') == 'Polygon':
                coord_count = len(coords[0]) if coords else 0
                print(f"   Nombre de coordonnées: {coord_count}")
            elif geometry.get('type') == 'MultiPolygon':
                coord_count = len(coords[0][0]) if coords and coords[0] else 0
                print(f"   Nombre de coordonnées (premier polygone): {coord_count}")
        
        if len(features) > 5:
            print(f"\n... et {len(features) - 5} autres features")
            
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse: {e}")

def generate_job_argument(ward_id: str, coordinates: List[List[float]], 
                         features: List[str] = None, 
                         area_prefix: str = "africa/south_africa/johannesburg",
                         custom_suffix: str = "johannesburg_custom_ward") -> str:
    """
    Génère un argument de job pour un ward donné
    
    Args:
        ward_id: ID du ward (ex: "79800001")
        coordinates: Liste des coordonnées [[lon,lat], [lon,lat], ...]
        features: Liste des features à inclure
        area_prefix: Préfixe pour le nom de l'area
        custom_suffix: Suffixe personnalisé pour le nom de l'area
    
    Returns:
        Argument de job formaté
    """
    if features is None:
        features = ["build", "card", "retrieve"]
    
    # Convertir les coordonnées en string sans espaces
    coords_str = str(coordinates).replace(' ', '')
    
    # Extraire le numéro du ward pour un nom plus lisible
    ward_number = ward_id.replace('79800', '') if ward_id.startswith('79800') else ward_id
    # Enlever les zéros en début si présents
    ward_number = ward_number.lstrip('0') or '0'
    
    # Générer le nom de l'area avec le numéro du ward
    area_name = f"{area_prefix}/{custom_suffix}_{ward_number}"
    
    # Générer les features
    features_str = f"[{','.join(features)}]"
    
    # Formater l'argument de job
    job_argument = f'"johannesburg-ward-{ward_number}" = [' \
                  f'"area.name={area_name}", ' \
                  f'"features={features_str}", ' \
                  f'"area.polygon={coords_str}"' \
                  f'],'
    
    return job_argument

def generate_all_job_arguments(ward_coordinates: Dict[str, List[List[float]]], 
                              output_file: str = "johannesburg/feature_bcr.json",
                              features: List[str] = None,
                              area_prefix: str = "africa/south_africa/johannesburg",
                              custom_suffix: str = "johannesburg_custom_ward") -> None:
    """
    Génère tous les arguments de job pour les wards
    """
    if features is None:
        features = ["build", "card", "retrieve"]
    
    print(f"🔧 GÉNÉRATION DES ARGUMENTS DE JOB POUR LES WARDS")
    print("=" * 60)
    
    job_arguments = []
    
    # Trier les wards par ID pour un ordre logique
    sorted_wards = sorted(ward_coordinates.items(), key=lambda x: x[0])
    
    for ward_id, coordinates in sorted_wards:
        job_arg = generate_job_argument(ward_id, coordinates, features, area_prefix, custom_suffix)
        job_arguments.append(job_arg)
        ward_number = ward_id.replace('79800', '') if ward_id.startswith('79800') else ward_id
        print(f"✅ Ward {ward_number} ({ward_id}): Argument de job généré")
    
    # Créer le contenu complet du fichier
    file_content = f"""
cli_args_per_job = {{
{chr(10).join(job_arguments)}
}}


"""
    
    # Créer le dossier johannesburg s'il n'existe pas
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Sauvegarder le fichier
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(file_content)
    
    print(f"\n💾 SAUVEGARDE:")
    print(f"   📄 Fichier: {output_file}")
    print(f"   📊 Arguments: {len(job_arguments)} wards")
    print(f"   🎯 Features: {features}")

def generate_custom_job_configs(ward_coordinates: Dict[str, List[List[float]]]) -> None:
    """
    Génère différentes configurations d'arguments de job
    """
    print(f"\n🎨 GÉNÉRATION DE CONFIGURATIONS PERSONNALISÉES")
    print("=" * 60)
    
    # Configuration 1: Build seulement
    print("\n1️⃣ Configuration BUILD uniquement...")
    generate_all_job_arguments(
        ward_coordinates, 
        "johannesburg/feature_bcr_build_only.json",
        ["build"]
    )
    
    # Configuration 2: Upload for annotation
    print("\n2️⃣ Configuration UPLOAD FOR ANNOTATION...")
    generate_all_job_arguments(
        ward_coordinates,
        "johannesburg/feature_bcr_annotation.json", 
        ["upload_for_annotation"]
    )
    
    # Configuration 3: Toutes les features
    print("\n3️⃣ Configuration COMPLÈTE...")
    generate_all_job_arguments(
        ward_coordinates,
        "johannesburg/feature_bcr_full.json", 
        ["build", "card", "retrieve"]
    )
    
    # Configuration 4: Par zones géographiques
    print("\n4️⃣ Configuration par ZONES...")
    generate_zone_job_configs(ward_coordinates)

def generate_zone_job_configs(ward_coordinates: Dict[str, List[List[float]]]) -> None:
    """
    Génère des configurations d'arguments de job séparées par zones géographiques
    Crée des blocs de 70 wards maximum par zone
    """
    
    # Définir les zones par blocs de 70 wards
    zones = {
        "zone_001_070": list(range(1, 71)),   # Wards 1-70
        "zone_071_140": list(range(71, 141)), # Wards 71-140
        "zone_141_210": list(range(141, 211)), # Wards 141-210
        "zone_211_280": list(range(211, 281)), # Wards 211-280
        "zone_281_350": list(range(281, 351)), # Wards 281-350
    }
    
    for zone_name, ward_numbers in zones.items():
        zone_wards = {}
        
        for ward_num in ward_numbers:
            # Essayer différents formats d'ID
            possible_ids = [
                f"79800{ward_num:03d}",  # Format: 79800001
                f"7980000{ward_num}",    # Format: 79800001 (pour les simples chiffres)
                str(ward_num)            # Format simple
            ]
            
            for ward_id in possible_ids:
                if ward_id in ward_coordinates:
                    zone_wards[ward_id] = ward_coordinates[ward_id]
                    break
        
        if zone_wards:
            zone_file = f"johannesburg/feature_bcr_{zone_name}.json"
            generate_all_job_arguments(
                zone_wards, 
                zone_file,
                custom_suffix="johannesburg_custom_ward"
            )
            print(f"   📍 Zone {zone_name}: {len(zone_wards)} wards → {zone_file}")

def print_sample_job_arguments(ward_coordinates: Dict[str, List[List[float]]], sample_count: int = 5) -> None:
    """
    Affiche quelques exemples d'arguments de job
    """
    print(f"\n📋 EXEMPLES D'ARGUMENTS DE JOB GÉNÉRÉS:")
    print("=" * 60)
    
    sample_wards = list(ward_coordinates.items())[:sample_count]
    
    for ward_id, coordinates in sample_wards:
        job_arg = generate_job_argument(ward_id, coordinates)
        print(job_arg)
    
    if len(ward_coordinates) > sample_count:
        print(f"   ... et {len(ward_coordinates) - sample_count} autres wards")

def create_ward_coordinates_sample() -> Dict[str, List[List[float]]]:
    """
    Crée un échantillon de coordonnées pour tester si le fichier n'existe pas
    """
    return {
        "79800001": [[27.72,-26.394166666666667],[27.774545454545454,-26.394166666666667],[27.774545454545454,-26.43],[27.72,-26.43],[27.72,-26.394166666666667]],
        "79800002": [[27.774545454545454,-26.394166666666667],[27.829090909090908,-26.394166666666667],[27.829090909090908,-26.43],[27.774545454545454,-26.43],[27.774545454545454,-26.394166666666667]],
        "79800003": [[27.829090909090908,-26.394166666666667],[27.883636363636363,-26.394166666666667],[27.883636363636363,-26.43],[27.829090909090908,-26.43],[27.829090909090908,-26.394166666666667]],
        "79800004": [[27.883636363636363,-26.394166666666667],[27.938181818181818,-26.394166666666667],[27.938181818181818,-26.43],[27.883636363636363,-26.43],[27.883636363636363,-26.394166666666667]],
        "79800005": [[27.938181818181818,-26.394166666666667],[27.992727272727272,-26.394166666666667],[27.992727272727272,-26.43],[27.938181818181818,-26.43],[27.938181818181818,-26.394166666666667]]
    }

def generate_single_ward_job(ward_id: str, coordinates: List[List[float]], 
                           features: List[str] = None) -> str:
    """
    Génère un argument de job pour un seul ward (utile pour tests)
    
    Args:
        ward_id: ID du ward
        coordinates: Coordonnées du polygone
        features: Features à utiliser
    
    Returns:
        Argument de job formaté
    """
    if features is None:
        features = ["build", "card", "retrieve"]
    
    job_arg = generate_job_argument(ward_id, coordinates, features)
    
    print(f"🎯 ARGUMENT DE JOB POUR LE WARD {ward_id}:")
    print("=" * 50)
    print(job_arg)
    
    return job_arg

def main():
    """
    Fonction principale
    """
    print("🏙️ GÉNÉRATEUR D'ARGUMENTS DE JOB POUR LES WARDS DE JOHANNESBURG")
    print("=" * 70)
    
    # Analyser le fichier GeoJSON pour comprendre sa structure
    print("🔍 ANALYSE DU FICHIER GEOJSON...")
    extract_ward_info_from_geojson()
    
    # Charger les coordonnées des wards depuis le GeoJSON
    print("\n📂 CHARGEMENT DES COORDONNÉES...")
    ward_coordinates = load_ward_coordinates_from_geojson()
    
    # Si le fichier n'existe pas, utiliser un échantillon
    if not ward_coordinates:
        print("⚠️  Fichier johannesburg_inv.geojson non trouvé")
        print("🔧 Utilisation d'un échantillon pour démonstration...")
        ward_coordinates = create_ward_coordinates_sample()
    
    print(f"📊 {len(ward_coordinates)} wards chargés")
    
    # Afficher des exemples
    print_sample_job_arguments(ward_coordinates)
    
    # Générer la configuration principale
    generate_all_job_arguments(ward_coordinates)
    
    # Générer les configurations personnalisées
    generate_custom_job_configs(ward_coordinates)
    
    print(f"\n🎉 GÉNÉRATION TERMINÉE!")
    print(f"✅ {len(ward_coordinates)} arguments de job générés")
    print(f"📁 Fichiers créés dans le répertoire johannesburg/")
    
    # Instructions d'utilisation
    print(f"\n📖 UTILISATION:")
    print(f"   1. Placez vos fichiers générés dans votre projet Terraform")
    print(f"   2. Copiez les arguments dans votre fichier terraform.auto.tfvars")
    print(f"   3. Utilisez: cli_args_per_job[\"johannesburg-ward-1\"] pour un ward spécifique")
    print(f"   4. Ou: for ward_id, args in cli_args_per_job.items() pour tous")
    
    # Exemple d'un seul ward
    if ward_coordinates:
        first_ward_id = list(ward_coordinates.keys())[0]
        first_ward_coords = ward_coordinates[first_ward_id]
        print(f"\n🔍 EXEMPLE D'ARGUMENT DE JOB:")
        generate_single_ward_job(first_ward_id, first_ward_coords)

if __name__ == "__main__":
    main()