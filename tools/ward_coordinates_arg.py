import json
import os
from typing import Dict, List, Optional

def load_ward_coordinates(file_path: str = "johannesburg/ward_coordinates.json") -> Dict[str, List[List[float]]]:
    """
    Charge les coordonnées des wards depuis le fichier JSON
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Fichier {file_path} non trouvé")
        return {}
    except json.JSONDecodeError:
        print(f"❌ Erreur de format JSON dans {file_path}")
        return {}

def generate_job_argument(ward_id: str, coordinates: List[List[float]], 
                         features: List[str] = None, 
                         area_prefix: str = "africa/south_africa/johannesburg",
                         custom_suffix: str = "johannesburg_custom") -> str:
    """
    Génère un argument de job pour un ward donné
    
    Args:
        ward_id: ID du ward (ex: "79001")
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
    
    # Générer le nom de l'area
    area_name = f"{area_prefix}/{custom_suffix}"
    
    # Générer les features
    features_str = f"[{','.join(features)}]"
    
    # Formater l'argument de job
    job_argument = f'"johannesburg-{ward_id}" = [' \
                  f'"area.name={area_name}", ' \
                  f'"features={features_str}", ' \
                  f'"area.polygon={coords_str}"' \
                  f'],'
    
    return job_argument

def generate_all_job_arguments(ward_coordinates: Dict[str, List[List[float]]], 
                              output_file: str = "johannesburg/feature_bcr.json",
                              features: List[str] = None,
                              area_prefix: str = "africa/south_africa/johannesburg",
                              custom_suffix: str = "johannesburg_custom") -> None:
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
        print(f"✅ {ward_id}: Argument de job généré")
    
    # Créer le contenu complet du fichier
    file_content = f"""# Arguments de job pour les wards de Johannesburg
# Généré automatiquement - {len(job_arguments)} wards
# Features: {features}

cli_args_per_job = {{
{chr(10).join(job_arguments)}
}}

# Utilisation:
# Pour accéder à un ward spécifique: cli_args_per_job["{sorted_wards[0][0] if sorted_wards else 'WARD_ID'}"]
# Pour itérer sur tous les wards: for ward_id, args in cli_args_per_job.items()
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
        ["build", "card", "retrieve", "upload_for_annotation", "predict"]
    )
    
    # Configuration 4: Par zones
    print("\n4️⃣ Configuration par ZONES...")
    generate_zone_job_configs(ward_coordinates)

def generate_zone_job_configs(ward_coordinates: Dict[str, List[List[float]]]) -> None:
    """
    Génère des configurations d'arguments de job séparées par zones géographiques
    """
    
    # Définir les zones basées sur les IDs des wards
    zones = {
        "soweto": list(range(1, 45)),      # Wards 79001-79044: Soweto et sud
        "roodepoort": list(range(45, 89)), # Wards 79045-79088: Roodepoort et centre-sud
        "sandton_cbd": list(range(89, 123)), # Wards 79089-79122: Centre et CBD
        "fourways": list(range(123, 133))  # Wards 79123-79132: Fourways et nord
    }
    
    for zone_name, ward_numbers in zones.items():
        zone_wards = {}
        
        for ward_num in ward_numbers:
            ward_id = f"79{ward_num:03d}"
            if ward_id in ward_coordinates:
                zone_wards[ward_id] = ward_coordinates[ward_id]
        
        if zone_wards:
            zone_file = f"johannesburg/feature_bcr_{zone_name}.json"
            generate_all_job_arguments(
                zone_wards, 
                zone_file,
                custom_suffix=f"johannesburg_{zone_name}"
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
        "79001": [[27.72,-26.394166666666667],[27.774545454545454,-26.394166666666667],[27.774545454545454,-26.43],[27.72,-26.43],[27.72,-26.394166666666667]],
        "79002": [[27.774545454545454,-26.394166666666667],[27.829090909090908,-26.394166666666667],[27.829090909090908,-26.43],[27.774545454545454,-26.43],[27.774545454545454,-26.394166666666667]],
        "79003": [[27.829090909090908,-26.394166666666667],[27.883636363636363,-26.394166666666667],[27.883636363636363,-26.43],[27.829090909090908,-26.43],[27.829090909090908,-26.394166666666667]],
        "79004": [[27.883636363636363,-26.394166666666667],[27.938181818181818,-26.394166666666667],[27.938181818181818,-26.43],[27.883636363636363,-26.43],[27.883636363636363,-26.394166666666667]],
        "79005": [[27.938181818181818,-26.394166666666667],[27.992727272727272,-26.394166666666667],[27.992727272727272,-26.43],[27.938181818181818,-26.43],[27.938181818181818,-26.394166666666667]]
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
    
    # Charger les coordonnées des wards
    ward_coordinates = load_ward_coordinates()
    
    # Si le fichier n'existe pas, utiliser un échantillon
    if not ward_coordinates:
        print("⚠️  Fichier ward_coordinates.json non trouvé")
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
    print(f"📁 Fichiers créés dans le répertoire courant")
    
    # Instructions d'utilisation
    print(f"\n📖 UTILISATION:")
    print(f"   1. Placez votre fichier feature_bcr.json dans le dossier johannesburg/")
    print(f"   2. Copiez les arguments dans votre fichier Terraform")
    print(f"   3. Utilisez: cli_args_per_job[\"79001\"] pour un ward spécifique")
    print(f"   4. Ou: for ward_id, args in cli_args_per_job.items() pour tous")
    
    # Exemple d'un seul ward
    if ward_coordinates:
        first_ward_id = list(ward_coordinates.keys())[0]
        first_ward_coords = ward_coordinates[first_ward_id]
        print(f"\n🔍 EXEMPLE D'ARGUMENT DE JOB:")
        generate_single_ward_job(first_ward_id, first_ward_coords)

if __name__ == "__main__":
    main()