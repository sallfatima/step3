#!/usr/bin/env python3
"""
Script pour générer automatiquement les arguments de configuration 
à partir des subdivisions de wards.

Usage:
    python config_args_generator.py --input johannesburg_ward_subdivisions_inv.geojson --output config_args.txt
    python config_args_generator.py --input johannesburg_ward_subdivisions_inv.geojson --format terraform
"""

import json
import os
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any


def format_coordinates_for_config(coordinates: List[List[float]]) -> str:
    """Formate les coordonnées pour la configuration"""
    if isinstance(coordinates[0][0], list):
        # Si c'est un polygone avec des anneaux, prendre l'anneau extérieur
        coords = coordinates[0]
    else:
        coords = coordinates
    
    # Formatter comme dans l'exemple: [lat,lon] pairs
    formatted_coords = []
    for coord in coords:
        lat, lon = coord[0], coord[1]  # Les coordonnées sont déjà inversées
        formatted_coords.append(f"[{lat},{lon}]")
    
    return "[" + ",".join(formatted_coords) + "]"


def generate_ward_config_name(ward_num: int, subdivision_id: int, subdivision_total: int) -> str:
    """Génère le nom de configuration pour un ward subdivision"""
    return f"johannesburg-ward-{ward_num}-sub-{subdivision_id}"


def generate_area_name(ward_num: int, subdivision_id: int) -> str:
    """Génère le nom de zone pour la configuration"""
    return f"africa/south_africa/johannesburg/johannesburg_custom_ward_{ward_num}_sub_{subdivision_id}"


def generate_terraform_config(subdivisions_data: List[Dict], output_format: str = "terraform") -> str:
    """Génère la configuration selon le format demandé"""
    
    if output_format == "terraform":
        lines = []
        lines.append("cli_args_per_job = {")
        
        for subdivision in subdivisions_data:
            ward_num = subdivision['ward_number']
            sub_id = subdivision['subdivision_id']
            coordinates_str = subdivision['coordinates_formatted']
            
            config_name = generate_ward_config_name(ward_num, sub_id, subdivision['subdivision_total'])
            area_name = generate_area_name(ward_num, sub_id)
            
            # Générer la ligne de configuration avec le bon format de liste
            config_line = f'  "{config_name}" = ["area.name={area_name}", "features=[build,card,retrieve]", "area.polygon={coordinates_str}"],'
            lines.append(config_line)
        
        lines.append("}")
        return "\n".join(lines)
    
    elif output_format == "json":
        config = {}
        for subdivision in subdivisions_data:
            ward_num = subdivision['ward_number']
            sub_id = subdivision['subdivision_id']
            coordinates_str = subdivision['coordinates_formatted']
            
            config_name = generate_ward_config_name(ward_num, sub_id, subdivision['subdivision_total'])
            area_name = generate_area_name(ward_num, sub_id)
            
            # Format correct: liste d'arguments comme dans l'exemple
            config[config_name] = [
                f"area.name={area_name}",
                "features=[build,card,retrieve]",
                f"area.polygon={coordinates_str}"
            ]
        
        return json.dumps(config, indent=2)
    
    elif output_format == "yaml":
        lines = []
        lines.append("cli_args_per_job:")
        
        for subdivision in subdivisions_data:
            ward_num = subdivision['ward_number']
            sub_id = subdivision['subdivision_id']
            coordinates_str = subdivision['coordinates_formatted']
            
            config_name = generate_ward_config_name(ward_num, sub_id, subdivision['subdivision_total'])
            area_name = generate_area_name(ward_num, sub_id)
            
            lines.append(f"  {config_name}:")
            lines.append(f"    - area.name={area_name}")
            lines.append(f"    - features=[build,card,retrieve]")
            lines.append(f"    - area.polygon={coordinates_str}")
        
        return "\n".join(lines)
    
    else:  # format simple/text
        lines = []
        for subdivision in subdivisions_data:
            ward_num = subdivision['ward_number']
            sub_id = subdivision['subdivision_id']
            coordinates_str = subdivision['coordinates_formatted']
            
            config_name = generate_ward_config_name(ward_num, sub_id, subdivision['subdivision_total'])
            area_name = generate_area_name(ward_num, sub_id)
            
            lines.append(f"# Ward {ward_num} - Subdivision {sub_id}")
            lines.append(f'"{config_name}" = [')
            lines.append(f'  "area.name={area_name}",')
            lines.append(f'  "features=[build,card,retrieve]",')
            lines.append(f'  "area.polygon={coordinates_str}"')
            lines.append(f'],')
            lines.append("")
        
        return "\n".join(lines)


def process_geojson_subdivisions(input_path: str, output_path: str = None, 
                                output_format: str = "terraform") -> Dict:
    """Traite le fichier GeoJSON des subdivisions et génère la configuration"""
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier {input_path} n'existe pas")
    
    print(f"📖 Lecture du fichier: {input_path}")
    print(f"📄 Format de sortie: {output_format}")
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        if geojson_data.get('type') != 'FeatureCollection':
            raise ValueError("Le fichier doit être un FeatureCollection GeoJSON")
        
        features = geojson_data.get('features', [])
        print(f"✅ {len(features)} subdivisions trouvées")
        
        subdivisions_data = []
        
        for feature in features:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            if geometry.get('type') != 'Polygon':
                print(f"⚠️  Géométrie non-polygone ignorée: {geometry.get('type')}")
                continue
            
            coordinates = geometry.get('coordinates', [])
            if not coordinates:
                print(f"⚠️  Coordonnées manquantes pour une subdivision")
                continue
            
            # Extraire les informations de la subdivision
            ward_num = properties.get('original_ward', properties.get('WardNo', 'unknown'))
            sub_id = properties.get('subdivision_id', 1)
            sub_total = properties.get('subdivision_total', 3)
            
            # Formatter les coordonnées
            coordinates_formatted = format_coordinates_for_config(coordinates)
            
            subdivision_info = {
                'ward_number': ward_num,
                'subdivision_id': sub_id,
                'subdivision_total': sub_total,
                'coordinates_raw': coordinates,
                'coordinates_formatted': coordinates_formatted,
                'properties': properties
            }
            
            subdivisions_data.append(subdivision_info)
            print(f"✅ Ward {ward_num} Sub {sub_id}: Coordonnées formatées")
        
        # Trier par ward puis par subdivision
        subdivisions_data.sort(key=lambda x: (x['ward_number'], x['subdivision_id']))
        
        # Générer la configuration
        print(f"🔄 Génération de la configuration...")
        config_content = generate_terraform_config(subdivisions_data, output_format)
        
        # Définir le fichier de sortie
        if output_path is None:
            input_name = Path(input_path).stem
            input_dir = Path(input_path).parent
            
            # Utiliser le même dossier que le fichier d'entrée par défaut
            if output_format == "terraform":
                output_path = input_dir / f"{input_name}_config.tf"
            elif output_format == "json":
                output_path = input_dir / f"ward_configs.json"
            elif output_format == "yaml":
                output_path = input_dir / f"{input_name}_config.yaml"
            else:
                output_path = input_dir / f"{input_name}_config.txt"
        
        # Créer le dossier de sortie si nécessaire
        output_dir = Path(output_path).parent
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        
        # Écrire le fichier de configuration
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(config_content)
        
        stats = {
            'input_file': input_path,
            'output_file': str(output_path),
            'subdivisions_processed': len(subdivisions_data),
            'output_format': output_format,
            'wards_found': list(set(sub['ward_number'] for sub in subdivisions_data)),
            'success': True
        }
        
        print(f"✅ Configuration générée: {output_path}")
        print(f"📊 {len(subdivisions_data)} subdivisions traitées")
        print(f"🎯 Wards: {sorted(stats['wards_found'])}")
        
        return stats
        
    except json.JSONDecodeError as e:
        error_msg = f"Erreur de format JSON: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}
    
    except Exception as e:
        error_msg = f"Erreur lors du traitement: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}


def preview_config(input_path: str, max_items: int = 3) -> None:
    """Affiche un aperçu de la configuration qui sera générée"""
    
    print(f"🔍 Aperçu de la configuration pour: {input_path}")
    print("-" * 60)
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        features = geojson_data.get('features', [])[:max_items]
        
        for i, feature in enumerate(features):
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            ward_num = properties.get('original_ward', properties.get('WardNo', 'unknown'))
            sub_id = properties.get('subdivision_id', 1)
            
            config_name = generate_ward_config_name(ward_num, sub_id, 3)
            area_name = generate_area_name(ward_num, sub_id)
            
            print(f"📋 Configuration {i+1}:")
            print(f"   Nom: {config_name}")
            print(f"   Zone: {area_name}")
            print(f"   Ward: {ward_num}, Subdivision: {sub_id}")
            
            if geometry.get('coordinates'):
                coord_count = len(geometry['coordinates'][0]) if geometry['coordinates'] else 0
                print(f"   Coordonnées: {coord_count} points")
            
            print()
        
        total_features = len(geojson_data.get('features', []))
        if total_features > max_items:
            print(f"... et {total_features - max_items} autres configurations")
            
    except Exception as e:
        print(f"❌ Erreur lors de l'aperçu: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description="Génère automatiquement les arguments de configuration à partir des subdivisions de wards",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:

  # Générer la configuration Terraform (par défaut)
  python %(prog)s -i johannesburg_ward_subdivisions_inv.geojson

  # Générer avec format et fichier de sortie spécifiques
  python %(prog)s -i subdivisions.geojson -o config.tf --format terraform

  # Générer en format JSON
  python %(prog)s -i subdivisions.geojson --format json

  # Aperçu de la configuration
  python %(prog)s -i subdivisions.geojson --preview

Formats supportés:
  - terraform: Format Terraform HCL (défaut)
  - json: Format JSON
  - yaml: Format YAML
  - text: Format texte simple
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='Chemin vers le fichier GeoJSON des subdivisions'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Fichier de sortie (généré automatiquement si non spécifié)'
    )
    
    parser.add_argument(
        '--format',
        choices=['terraform', 'json', 'yaml', 'text'],
        default='terraform',
        help='Format de sortie (défaut: terraform)'
    )
    
    parser.add_argument(
        '--preview',
        action='store_true',
        help='Afficher un aperçu sans générer le fichier'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Mode verbeux'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"🚀 Génération des arguments de configuration")
        print(f"📂 Fichier d'entrée: {args.input}")
        print(f"📄 Format: {args.format}")
        print(f"📁 Sortie: {args.output or 'Auto-généré'}")
        print("-" * 50)
    
    if args.preview:
        preview_config(args.input)
        return
    
    try:
        stats = process_geojson_subdivisions(
            input_path=args.input,
            output_path=args.output,
            output_format=args.format
        )
        
        if stats['success']:
            print(f"\n🌟 Configuration générée avec succès!")
            print(f"📄 Fichier: {stats['output_file']}")
            print(f"📊 {stats['subdivisions_processed']} subdivisions configurées")
            print(f"🎯 Wards traités: {sorted(stats['wards_found'])}")
            sys.exit(0)
        else:
            print(f"\n❌ Échec: {stats['error']}")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 Erreur critique: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()