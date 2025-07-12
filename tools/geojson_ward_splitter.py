#!/usr/bin/env python3
"""
Script en ligne de commande pour diviser un fichier GeoJSON par ward
et inverser optionnellement les coordonnées.

Usage:
    python geojson_ward_splitter.py --input data.geojson --wards 1,2,3 --output ./wards
    python geojson_ward_splitter.py -i johannesburg.geojson -w 1,2,3,5,10 --invert
    python geojson_ward_splitter.py --help
"""

import json
import os
import argparse
import sys
from pathlib import Path
from typing import List, Union, Any, Dict


def invert_coordinates(coordinates: Union[List, tuple]) -> List:
    """Inverse récursivement les coordonnées [lon, lat] vers [lat, lon]"""
    if isinstance(coordinates[0], (list, tuple)):
        return [invert_coordinates(coord) for coord in coordinates]
    else:
        return [coordinates[1], coordinates[0]]


def process_geojson_coordinates(geojson_data: dict) -> tuple[dict, int]:
    """Traite un objet GeoJSON et inverse toutes les coordonnées"""
    import copy
    result = copy.deepcopy(geojson_data)
    coordinate_count = 0
    
    if result.get('type') == 'FeatureCollection':
        for feature in result.get('features', []):
            if 'geometry' in feature and feature['geometry'] is not None:
                geometry = feature['geometry']
                if 'coordinates' in geometry and geometry['coordinates'] is not None:
                    geometry['coordinates'] = invert_coordinates(geometry['coordinates'])
                    coord_str = json.dumps(geometry['coordinates'])
                    coordinate_count += coord_str.count(',') // 2
    
    elif result.get('type') == 'Feature':
        if 'geometry' in result and result['geometry'] is not None:
            geometry = result['geometry']
            if 'coordinates' in geometry and geometry['coordinates'] is not None:
                geometry['coordinates'] = invert_coordinates(geometry['coordinates'])
    
    elif result.get('type') in ['Point', 'LineString', 'Polygon', 'MultiPoint', 
                                'MultiLineString', 'MultiPolygon']:
        if 'coordinates' in result and result['coordinates'] is not None:
            result['coordinates'] = invert_coordinates(result['coordinates'])
    
    return result, coordinate_count


def split_geojson_by_ward(input_path: str, ward_list: List[int], 
                         output_directory: str = None, invert_coords: bool = True) -> Dict:
    """Divise un fichier GeoJSON en plusieurs fichiers basés sur les numéros de ward"""
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier {input_path} n'existe pas")
    
    if output_directory is None:
        output_directory = str(Path(input_path).parent)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    print(f"📖 Lecture du fichier: {input_path}")
    print(f"🎯 Wards à extraire: {ward_list}")
    print(f"📁 Dossier de sortie: {output_directory}")
    print(f"🔄 Inversion coordonnées: {'Oui' if invert_coords else 'Non'}")
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print(f"✅ Fichier chargé avec succès")
        
        if geojson_data.get('type') != 'FeatureCollection':
            raise ValueError("Le fichier doit être un FeatureCollection GeoJSON")
        
        ward_features = {ward: [] for ward in ward_list}
        total_features = len(geojson_data.get('features', []))
        found_wards = set()
        
        print(f"🔍 Analyse de {total_features} features...")
        
        for feature in geojson_data.get('features', []):
            properties = feature.get('properties', {})
            ward_number = None
            possible_ward_fields = ['WardNo', 'Ward', 'ward_no', 'ward', 'WARD_NO', 'WARD']
            
            for field in possible_ward_fields:
                if field in properties:
                    try:
                        ward_number = int(properties[field])
                        break
                    except (ValueError, TypeError):
                        continue
            
            if ward_number is not None and ward_number in ward_list:
                ward_features[ward_number].append(feature)
                found_wards.add(ward_number)
        
        stats = {
            'input_file': input_path,
            'output_directory': output_directory,
            'requested_wards': ward_list,
            'found_wards': list(found_wards),
            'missing_wards': [w for w in ward_list if w not in found_wards],
            'total_input_features': total_features,
            'ward_files_created': [],
            'coordinates_inverted': invert_coords,
            'success': True
        }
        
        for ward_num in ward_list:
            features = ward_features[ward_num]
            
            if not features:
                print(f"⚠️  Ward {ward_num}: Aucune feature trouvée")
                continue
            
            print(f"📝 Ward {ward_num}: {len(features)} feature(s) trouvée(s)")
            
            ward_geojson = {
                "type": "FeatureCollection",
                "crs": geojson_data.get("crs"),
                "features": features
            }
            
            coordinate_count = 0
            if invert_coords:
                print(f"🔄 Ward {ward_num}: Inversion des coordonnées...")
                ward_geojson, coordinate_count = process_geojson_coordinates(ward_geojson)
            
            input_name = Path(input_path).stem
            coord_suffix = "_inv" if invert_coords else ""
            output_filename = f"{input_name}_ward_{ward_num}{coord_suffix}.geojson"
            output_path = os.path.join(output_directory, output_filename)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(ward_geojson, f, indent=2, ensure_ascii=False)
            
            ward_stats = {
                'ward_number': ward_num,
                'features': len(features),
                'coordinates_processed': coordinate_count,
                'output_file': output_path
            }
            stats['ward_files_created'].append(ward_stats)
            
            print(f"✅ Ward {ward_num}: Fichier créé - {output_filename}")
            if invert_coords:
                print(f"   📍 Coordonnées inversées: {coordinate_count:,}")
        
        print(f"\n🎉 Traitement terminé!")
        print(f"   📊 Wards demandés: {len(ward_list)}")
        print(f"   ✅ Wards trouvés: {len(found_wards)}")
        print(f"   📁 Fichiers créés: {len(stats['ward_files_created'])}")
        
        if stats['missing_wards']:
            print(f"   ⚠️  Wards non trouvés: {stats['missing_wards']}")
        
        return stats
        
    except json.JSONDecodeError as e:
        error_msg = f"Erreur de format JSON: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}
    
    except Exception as e:
        error_msg = f"Erreur lors du traitement: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}


def parse_ward_list(ward_string: str) -> List[int]:
    """Parse une chaîne de wards séparés par des virgules"""
    try:
        if '-' in ward_string:
            # Support pour les ranges comme "1-5"
            parts = ward_string.split('-')
            if len(parts) == 2:
                start, end = int(parts[0]), int(parts[1])
                return list(range(start, end + 1))
        
        # Support pour les listes comme "1,2,3,5,10"
        return [int(w.strip()) for w in ward_string.split(',') if w.strip()]
    
    except ValueError:
        raise argparse.ArgumentTypeError(f"Format de ward invalide: {ward_string}")


def main():
    parser = argparse.ArgumentParser(
        description="Divise un fichier GeoJSON par ward et inverse optionnellement les coordonnées",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:

  # Extraire les wards 1, 2, 3 avec inversion de coordonnées
  python %(prog)s -i johannesburg.geojson -w 1,2,3 --invert

  # Extraire une plage de wards (1 à 10)
  python %(prog)s -i data.geojson -w 1-10 -o ./output

  # Extraire des wards spécifiques sans inverser les coordonnées
  python %(prog)s --input city.geojson --wards 1,5,10,15 --no-invert

  # Mode verbeux avec statistiques détaillées
  python %(prog)s -i data.geojson -w 1,2,3 -v --stats

Formats supportés pour les wards:
  - Liste: 1,2,3,5,10
  - Plage: 1-10 (wards 1 à 10 inclus)
  - Mixte: 1,2,5-8,10 (pas encore supporté)
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='Chemin vers le fichier GeoJSON d\'entrée'
    )
    
    parser.add_argument(
        '-w', '--wards',
        required=True,
        type=parse_ward_list,
        help='Liste des wards à extraire (ex: 1,2,3 ou 1-10)'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Dossier de sortie (par défaut: même dossier que l\'entrée)'
    )
    
    parser.add_argument(
        '--invert', '--inv',
        action='store_true',
        default=True,
        help='Inverse les coordonnées [lon,lat] -> [lat,lon] (défaut)'
    )
    
    parser.add_argument(
        '--no-invert', '--no-inv',
        action='store_true',
        help='Ne pas inverser les coordonnées'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Mode verbeux'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Afficher les statistiques détaillées'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulation (ne crée pas les fichiers)'
    )
    
    args = parser.parse_args()
    
    # Gestion de l'inversion des coordonnées
    invert_coords = args.invert and not args.no_invert
    
    if args.verbose:
        print(f"🚀 Démarrage du traitement GeoJSON")
        print(f"📂 Fichier d'entrée: {args.input}")
        print(f"🎯 Wards: {args.wards}")
        print(f"📁 Sortie: {args.output or 'Même dossier'}")
        print(f"🔄 Inversion: {'Oui' if invert_coords else 'Non'}")
        print(f"🧪 Simulation: {'Oui' if args.dry_run else 'Non'}")
        print("-" * 50)
    
    if args.dry_run:
        print("🧪 MODE SIMULATION - Aucun fichier ne sera créé")
        # Ici on pourrait juste analyser sans écrire
        return
    
    try:
        stats = split_geojson_by_ward(
            input_path=args.input,
            ward_list=args.wards,
            output_directory=args.output,
            invert_coords=invert_coords
        )
        
        if stats['success']:
            if args.stats:
                print(f"\n📊 STATISTIQUES DÉTAILLÉES:")
                print(f"   📂 Fichier source: {stats['input_file']}")
                print(f"   📁 Dossier sortie: {stats['output_directory']}")
                print(f"   🎯 Wards demandés: {len(stats['requested_wards'])}")
                print(f"   ✅ Wards trouvés: {len(stats['found_wards'])}")
                print(f"   📄 Fichiers créés: {len(stats['ward_files_created'])}")
                
                if stats['missing_wards']:
                    print(f"   ⚠️  Wards manquants: {stats['missing_wards']}")
                
                print(f"\n📋 DÉTAIL PAR WARD:")
                for ward_info in stats['ward_files_created']:
                    print(f"   • Ward {ward_info['ward_number']}: {ward_info['features']} features")
                    if invert_coords:
                        print(f"     🔄 {ward_info['coordinates_processed']:,} coordonnées inversées")
                    print(f"     📄 {os.path.basename(ward_info['output_file'])}")
            
            print(f"\n🌟 Traitement réussi! {len(stats['ward_files_created'])} fichier(s) créé(s)")
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