#!/usr/bin/env python3
"""
Script pour convertir les coordonnées d'un fichier JSON de [longitude, latitude] vers [latitude, longitude]
Usage: python coordinate_converter.py input.json output.json
"""

import json
import sys
import argparse
import re
from typing import Dict, Any, List

def swap_coordinates_in_polygon_string(polygon_str: str) -> str:
    """
    Convertit les coordonnées dans une chaîne de polygone de [lon,lat] vers [lat,lon]
    
    Args:
        polygon_str: Chaîne contenant le polygone au format "area.polygon=[[lon,lat],...]"
    
    Returns:
        Chaîne avec coordonnées inversées
    """
    # Pattern pour matcher les coordonnées [longitude, latitude]
    coord_pattern = r'\[([+-]?\d+\.?\d*),([+-]?\d+\.?\d*)\]'
    
    def swap_coords(match):
        lon, lat = match.groups()
        # Inverser l'ordre : [longitude, latitude] -> [latitude, longitude]
        return f'[{lat},{lon}]'
    
    # Remplacer toutes les coordonnées dans le polygone
    converted = re.sub(coord_pattern, swap_coords, polygon_str)
    return converted

def swap_coordinates_in_list(coords_list: List[List[float]]) -> List[List[float]]:
    """
    Convertit une liste de coordonnées de [lon,lat] vers [lat,lon]
    
    Args:
        coords_list: Liste de coordonnées [[lon, lat], ...]
    
    Returns:
        Liste avec coordonnées inversées [[lat, lon], ...]
    """
    return [[coord[1], coord[0]] for coord in coords_list]

def process_cli_args(args_list: List[str]) -> List[str]:
    """
    Traite une liste d'arguments CLI et inverse les coordonnées dans area.polygon
    
    Args:
        args_list: Liste des arguments CLI
    
    Returns:
        Liste des arguments avec coordonnées inversées
    """
    processed_args = []
    
    for arg in args_list:
        if arg.startswith("area.polygon="):
            # Convertir les coordonnées dans cette chaîne
            converted_arg = swap_coordinates_in_polygon_string(arg)
            processed_args.append(converted_arg)
        else:
            # Garder l'argument tel quel
            processed_args.append(arg)
    
    return processed_args

def convert_coordinates_in_data(data: Any) -> Any:
    """
    Convertit récursivement les coordonnées dans une structure de données
    
    Args:
        data: Données à traiter (dict, list, ou autre)
    
    Returns:
        Données avec coordonnées inversées
    """
    if isinstance(data, dict):
        converted_data = {}
        for key, value in data.items():
            converted_data[key] = convert_coordinates_in_data(value)
        return converted_data
    
    elif isinstance(data, list):
        # Si c'est une liste d'arguments CLI, les traiter spécialement
        if len(data) > 0 and isinstance(data[0], str) and any("area.polygon=" in item for item in data):
            return process_cli_args(data)
        
        # Sinon, traiter récursivement
        return [convert_coordinates_in_data(item) for item in data]
    
    else:
        # Pour les autres types (str, int, float, etc.), retourner tel quel
        return data

def convert_json_file(input_file: str, output_file: str) -> None:
    """
    Convertit un fichier JSON avec inversion des coordonnées
    
    Args:
        input_file: Chemin du fichier JSON d'entrée
        output_file: Chemin du fichier JSON de sortie
    """
    try:
        # Lire le fichier JSON d'entrée
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"✅ Fichier lu: {input_file}")
        
        # Convertir les coordonnées
        converted_data = convert_coordinates_in_data(data)
        
        # Compter le nombre de wards traités
        if isinstance(converted_data, dict) and 'cli_args_per_job' in converted_data:
            ward_count = len(converted_data['cli_args_per_job'])
            print(f"✅ {ward_count} wards traités")
        
        # Écrire le fichier JSON de sortie
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(converted_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Fichier converti sauvegardé: {output_file}")
        print("🔄 Coordonnées converties de [longitude, latitude] vers [latitude, longitude]")
        
    except FileNotFoundError:
        print(f"❌ Erreur: Le fichier {input_file} n'existe pas.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Erreur: Le fichier {input_file} n'est pas un JSON valide.")
        print(f"   Détail: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Convertit les coordonnées d'un fichier JSON de [longitude, latitude] vers [latitude, longitude]",
        epilog="Exemple: python coordinate_converter.py johannesburg_wards.json johannesburg_wards_converted.json"
    )
    
    parser.add_argument(
        'input_file',
        help='Fichier JSON d\'entrée contenant les coordonnées à convertir'
    )
    
    parser.add_argument(
        'output_file',
        help='Fichier JSON de sortie avec les coordonnées converties'
    )
    
    parser.add_argument(
        '--preview',
        action='store_true',
        help='Afficher un aperçu des conversions sans sauvegarder'
    )
    
    args = parser.parse_args()
    
    if args.preview:
        # Mode aperçu
        try:
            with open(args.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print("🔍 APERÇU DES CONVERSIONS:")
            print("=" * 50)
            
            # Montrer quelques exemples de conversion
            if isinstance(data, dict) and 'cli_args_per_job' in data:
                for ward_name, ward_args in list(data['cli_args_per_job'].items())[:3]:  # Montrer 3 premiers
                    print(f"\n📍 {ward_name}:")
                    for arg in ward_args:
                        if arg.startswith("area.polygon="):
                            # Extraire les 2 premières coordonnées pour l'aperçu
                            coords_match = re.search(r'\[\[([+-]?\d+\.?\d*),([+-]?\d+\.?\d*)\],\[([+-]?\d+\.?\d*),([+-]?\d+\.?\d*)\]', arg)
                            if coords_match:
                                lon1, lat1, lon2, lat2 = coords_match.groups()
                                print(f"   Avant: [{lon1},{lat1}], [{lon2},{lat2}], ...")
                                print(f"   Après: [{lat1},{lon1}], [{lat2},{lon2}], ...")
                            break
            
            print(f"\n✨ Utilisez sans --preview pour convertir le fichier complet.")
            
        except Exception as e:
            print(f"❌ Erreur lors de l'aperçu: {e}")
            sys.exit(1)
    else:
        # Mode conversion complète
        print("🚀 CONVERSION DES COORDONNÉES")
        print("=" * 50)
        convert_json_file(args.input_file, args.output_file)

if __name__ == "__main__":
    main()