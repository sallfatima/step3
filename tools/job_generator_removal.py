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

def generate_removal_job_argument(ward_id: str, coordinates: List[List[float]], 
                                area_prefix: str = "africa/south_africa/johannesburg",
                                custom_suffix: str = "johannesburg_custom_ward",
                                removal_actions: List[str] = None,
                                annotations_filename: str = None,
                                custom_polygon_filename: str = None) -> str:
    """
    Génère un argument de job de removal pour un ward donné
    
    Args:
        ward_id: ID du ward (ex: "79800001")
        coordinates: Liste des coordonnées [[lon,lat], [lon,lat], ...]
        area_prefix: Préfixe pour le nom de l'area
        custom_suffix: Suffixe personnalisé pour le nom de l'area
        removal_actions: Liste des actions de removal (image_removal, location_removal, export)
        annotations_filename: Nom du fichier d'annotations (généré automatiquement si None)
        custom_polygon_filename: Nom du fichier de polygone personnalisé (généré automatiquement si None)
    
    Returns:
        Argument de job formaté pour removal
    """
    # Actions par défaut pour removal
    if removal_actions is None:
        removal_actions = ["image_removal", "location_removal", "export"]
    
    # Convertir les coordonnées en string sans espaces
    coords_str = str(coordinates).replace(' ', '')
    
    # Extraire le numéro du ward pour un nom plus lisible
    ward_number = ward_id.replace('79800', '') if ward_id.startswith('79800') else ward_id
    # Enlever les zéros en début si présents
    ward_number = ward_number.lstrip('0') or '0'
    
    # Générer le nom de l'area avec le numéro du ward
    area_name = f"{area_prefix}/{custom_suffix}_{ward_number}"
    
    # Générer le nom du fichier d'annotations spécifique au ward si non fourni
    if annotations_filename is None:
        annotations_filename = f"annotations_ward_{ward_number}.json"
    
    # Générer le nom du fichier de polygone personnalisé si non fourni
    if custom_polygon_filename is None:
        custom_polygon_filename = f"polygon_custom{ward_number}.geojson"
    
    # Formater les actions de removal
    removal_actions_str = f"[{','.join(removal_actions)}]"
    
    # Formater l'argument de job pour removal
    job_parts = [
        f'"area.name={area_name}"',
        f'"removal_export={removal_actions_str}"',
        f'"annotations_filename={annotations_filename}"',
        f'"custom_polygon_filename={custom_polygon_filename}"'
    ]
    
    job_argument = f'"johannesburg-ward-{ward_number}" = [' + ', '.join(job_parts) + '],'
    
    return job_argument

def generate_all_removal_jobs(ward_coordinates: Dict[str, List[List[float]]], 
                            output_file: str = "johannesburg/removal_export.json",
                            area_prefix: str = "africa/south_africa/johannesburg",
                            custom_suffix: str = "johannesburg_custom_ward",
                            removal_actions: List[str] = None,
                            annotations_filename: str = None,
                            custom_polygon_filename: str = None) -> None:
    """
    Génère tous les arguments de job de removal pour les wards
    """
    if removal_actions is None:
        removal_actions = ["image_removal", "location_removal", "export"]
    
    print(f"🗑️ GÉNÉRATION DES ARGUMENTS DE JOB REMOVAL/EXPORT")
    print("=" * 60)
    
    job_arguments = []
    
    # Trier les wards par ID pour un ordre logique
    sorted_wards = sorted(ward_coordinates.items(), key=lambda x: x[0])
    
    for ward_id, coordinates in sorted_wards:
        job_arg = generate_removal_job_argument(
            ward_id, coordinates, area_prefix, custom_suffix, removal_actions, 
            annotations_filename, custom_polygon_filename
        )
        job_arguments.append(job_arg)
        ward_number = ward_id.replace('79800', '') if ward_id.startswith('79800') else ward_id
        ward_number = ward_number.lstrip('0') or '0'
        annotations_name = annotations_filename or f"annotations_ward_{ward_number}.json"
        polygon_name = custom_polygon_filename or f"polygon_custom{ward_number}.geojson"
        print(f"✅ Ward {ward_number} ({ward_id}): Job removal généré")
        print(f"   🗑️ Actions: {removal_actions}")
        print(f"   📝 Annotations: {annotations_name}")
        print(f"   📐 Polygone: {polygon_name}")
    
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
    print(f"   📊 Arguments: {len(job_arguments)} jobs de removal")
    print(f"   🗑️ Actions: {removal_actions}")
    print(f"   🚀 Pipeline: REMOVAL/EXPORT")

def generate_removal_zone_configs(ward_coordinates: Dict[str, List[List[float]]]) -> None:
    """
    Génère des configurations de removal séparées par zones géographiques
    Crée des blocs de 70 wards maximum par zone pour les removals
    """
    print(f"\n🎯 GÉNÉRATION DE CONFIGURATIONS PAR ZONES")
    print("=" * 60)
    
    # Définir les zones par blocs de 70 wards pour le removal
    zones = {
        "zone_001_070": {
            "wards": list(range(1, 71)),
            "annotations": "annotations_zone_1.json"
        },
        "zone_071_140": {
            "wards": list(range(71, 141)),
            "annotations": "annotations_zone_2.json"
        },
        "zone_141_210": {
            "wards": list(range(141, 211)),
            "annotations": "annotations_zone_3.json"
        },
        "zone_211_280": {
            "wards": list(range(211, 281)),
            "annotations": "annotations_zone_4.json"
        },
        "zone_281_350": {
            "wards": list(range(281, 351)),
            "annotations": "annotations_zone_5.json"
        }
    }
    
    for zone_name, zone_config in zones.items():
        zone_wards = {}
        ward_numbers = zone_config["wards"]
        annotations_file = zone_config["annotations"]
        
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
            zone_file = f"johannesburg/removal_export_{zone_name}.json"
            generate_all_removal_jobs(
                zone_wards, 
                zone_file,
                annotations_filename=annotations_file,
                custom_polygon_filename=f"polygon_zone_{zone_name.split('_')[-1]}.geojson"
            )
            print(f"   🎯 Zone {zone_name}: {len(zone_wards)} wards → {zone_file} (annotations: {annotations_file})")

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

def print_sample_removal_jobs(ward_coordinates: Dict[str, List[List[float]]], sample_count: int = 3) -> None:
    """
    Affiche quelques exemples d'arguments de job de removal
    """
    print(f"\n📋 EXEMPLES D'ARGUMENTS DE JOB REMOVAL GÉNÉRÉS:")
    print("=" * 60)
    
    sample_wards = list(ward_coordinates.items())[:sample_count]
    
    for ward_id, coordinates in sample_wards:
        job_arg = generate_removal_job_argument(ward_id, coordinates)
        print(job_arg)
    
    if len(ward_coordinates) > sample_count:
        print(f"   ... et {len(ward_coordinates) - sample_count} autres wards")

def generate_single_removal_job(ward_id: str, coordinates: List[List[float]], 
                              removal_actions: List[str] = None,
                              annotations_filename: str = None,
                              custom_polygon_filename: str = None) -> str:
    """
    Génère un argument de job de removal pour un seul ward (utile pour tests)
    
    Args:
        ward_id: ID du ward
        coordinates: Coordonnées du polygone
        removal_actions: Liste des actions de removal
        annotations_filename: Nom du fichier d'annotations
        custom_polygon_filename: Nom du fichier de polygone personnalisé
    
    Returns:
        Argument de job formaté
    """
    job_arg = generate_removal_job_argument(ward_id, coordinates, 
                                          removal_actions=removal_actions,
                                          annotations_filename=annotations_filename,
                                          custom_polygon_filename=custom_polygon_filename)
    
    print(f"🎯 ARGUMENT DE JOB REMOVAL POUR LE WARD {ward_id}:")
    print("=" * 50)
    print(job_arg)
    
    return job_arg

def generate_custom_removal_configs(ward_coordinates: Dict[str, List[List[float]]]) -> None:
    """
    Génère différentes configurations de removal personnalisées
    """
    print(f"\n🎨 GÉNÉRATION DE CONFIGURATIONS PERSONNALISÉES")
    print("=" * 60)
    
    # Configuration 1: Image removal seulement
    print("\n1️⃣ Configuration IMAGE REMOVAL seulement...")
    generate_all_removal_jobs(
        ward_coordinates, 
        "johannesburg/removal_image_only.json",
        removal_actions=["image_removal"],
        annotations_filename="annotations_image_removal.json"
    )
    
    # Configuration 2: Location removal seulement
    print("\n2️⃣ Configuration LOCATION REMOVAL seulement...")
    generate_all_removal_jobs(
        ward_coordinates,
        "johannesburg/removal_location_only.json",
        removal_actions=["location_removal"],
        annotations_filename="annotations_location_removal.json"
    )
    
    # Configuration 3: Export seulement
    print("\n3️⃣ Configuration EXPORT seulement...")
    generate_all_removal_jobs(
        ward_coordinates,
        "johannesburg/removal_export_only.json",
        removal_actions=["export"],
        annotations_filename="annotations_export_only.json"
    )
    
    # Configuration 4: Removal complet avec export
    print("\n4️⃣ Configuration REMOVAL COMPLET...")
    generate_all_removal_jobs(
        ward_coordinates,
        "johannesburg/removal_complete.json",
        removal_actions=["image_removal", "location_removal", "export"],
        annotations_filename="annotations_complete_removal.json"
    )
    
    # Configuration 5: Par zones
    # print("\n5️⃣ Configuration par ZONES...")
    # generate_removal_zone_configs(ward_coordinates)

def main():
    """
    Fonction principale pour la génération des jobs de removal
    """
    print("🗑️ GÉNÉRATEUR D'ARGUMENTS DE JOB REMOVAL/EXPORT - JOHANNESBURG")
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
    print_sample_removal_jobs(ward_coordinates)
    
    # Générer la configuration principale pour REMOVAL
    print(f"\n🚀 GÉNÉRATION DE LA CONFIGURATION PRINCIPALE")
    print("=" * 60)
    generate_all_removal_jobs(ward_coordinates)
    
    # Générer les configurations personnalisées
    generate_custom_removal_configs(ward_coordinates)
    
    print(f"\n🎉 GÉNÉRATION TERMINÉE!")
    print(f"✅ {len(ward_coordinates)} arguments de job de removal générés")
    print(f"📁 Fichiers créés dans le répertoire johannesburg/")
    
    # Instructions d'utilisation
    print(f"\n📖 UTILISATION:")
    print(f"   1. Placez vos fichiers générés dans votre projet Terraform")
    print(f"   2. Copiez les arguments dans votre fichier terraform.auto.tfvars")
    print(f"   3. Utilisez: cli_args_per_job[\"johannesburg-ward-1\"] pour un ward spécifique")
    print(f"   4. Fichier principal: johannesburg/removal_export.json")
    print(f"   5. Actions disponibles: image_removal, location_removal, export")
    print(f"   6. Chaque ward aura son fichier: annotations_ward_n.json")
    print(f"   7. Chaque ward aura son polygone: polygon_customN.geojson")
    print(f"   8. Ou: for ward_id, args in cli_args_per_job.items() pour tous")
    
    # Exemple d'un seul ward
    if ward_coordinates:
        first_ward_id = list(ward_coordinates.keys())[0]
        first_ward_coords = ward_coordinates[first_ward_id]
        print(f"\n🔍 EXEMPLE D'ARGUMENT DE JOB REMOVAL:")
        generate_single_removal_job(first_ward_id, first_ward_coords)

if __name__ == "__main__":
    main()