#!/usr/bin/env python3
"""
Script pour diviser chaque ward en 3 subdivisions avec inversion optionnelle des coordonnées.

Usage:
    python ward_subdivider.py --input johannesburg.geojson --wards 25,89,93,97,112,120,124,125,132 --subdivisions 3
"""

import json
import os
import argparse
import sys
from pathlib import Path
from typing import List, Union, Any, Dict, Tuple
import math


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
    
    return result, coordinate_count


def get_polygon_bounds(coordinates: List[List[float]]) -> Tuple[float, float, float, float]:
    """Obtient les limites d'un polygone [min_lon, min_lat, max_lon, max_lat]"""
    if isinstance(coordinates[0][0], list):
        # Si c'est un polygone avec des anneaux (exterior + holes)
        coords = coordinates[0]  # Prendre l'anneau extérieur
    else:
        coords = coordinates
    
    lons = [coord[0] for coord in coords]
    lats = [coord[1] for coord in coords]
    
    return min(lons), min(lats), max(lons), max(lats)


def create_subdivision_grid(min_lon: float, min_lat: float, max_lon: float, max_lat: float, 
                           subdivisions: int) -> List[Tuple[float, float, float, float]]:
    """Crée une grille de subdivision"""
    if subdivisions == 3:
        # Division en 3 parties: 2x2 grille mais on ne prend que 3 cellules
        lon_step = (max_lon - min_lon) / 2
        lat_step = (max_lat - min_lat) / 2
        
        cells = []
        # Cellule 1: coin supérieur gauche
        cells.append((min_lon, min_lat + lat_step, min_lon + lon_step, max_lat))
        # Cellule 2: coin supérieur droit
        cells.append((min_lon + lon_step, min_lat + lat_step, max_lon, max_lat))
        # Cellule 3: partie inférieure (toute la largeur)
        cells.append((min_lon, min_lat, max_lon, min_lat + lat_step))
        
        return cells
    
    elif subdivisions == 4:
        # Division en 4 parties: grille 2x2
        lon_step = (max_lon - min_lon) / 2
        lat_step = (max_lat - min_lat) / 2
        
        cells = []
        for i in range(2):
            for j in range(2):
                cell = (
                    min_lon + j * lon_step,
                    min_lat + i * lat_step,
                    min_lon + (j + 1) * lon_step,
                    min_lat + (i + 1) * lat_step
                )
                cells.append(cell)
        return cells
    
    else:
        # Division linéaire pour d'autres nombres
        if subdivisions <= 3:
            # Division horizontale
            lat_step = (max_lat - min_lat) / subdivisions
            return [
                (min_lon, min_lat + i * lat_step, max_lon, min_lat + (i + 1) * lat_step)
                for i in range(subdivisions)
            ]
        else:
            # Division en grille
            grid_size = math.ceil(math.sqrt(subdivisions))
            lon_step = (max_lon - min_lon) / grid_size
            lat_step = (max_lat - min_lat) / grid_size
            
            cells = []
            for i in range(grid_size):
                for j in range(grid_size):
                    if len(cells) >= subdivisions:
                        break
                    cell = (
                        min_lon + j * lon_step,
                        min_lat + i * lat_step,
                        min_lon + (j + 1) * lon_step,
                        min_lat + (i + 1) * lat_step
                    )
                    cells.append(cell)
                if len(cells) >= subdivisions:
                    break
            return cells[:subdivisions]


def point_in_bbox(lon: float, lat: float, bbox: Tuple[float, float, float, float]) -> bool:
    """Vérifie si un point est dans une bounding box"""
    min_lon, min_lat, max_lon, max_lat = bbox
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def polygon_intersects_bbox(coordinates: List[List[float]], bbox: Tuple[float, float, float, float]) -> bool:
    """Vérifie si un polygone intersecte avec une bounding box (méthode simple)"""
    if isinstance(coordinates[0][0], list):
        coords = coordinates[0]  # Prendre l'anneau extérieur
    else:
        coords = coordinates
    
    # Vérifier si au moins un point du polygone est dans la bbox
    for coord in coords:
        if point_in_bbox(coord[0], coord[1], bbox):
            return True
    
    # Vérifier si la bbox est entièrement dans le polygone (cas simple)
    min_lon, min_lat, max_lon, max_lat = bbox
    bbox_center = ((min_lon + max_lon) / 2, (min_lat + max_lat) / 2)
    
    # Point in polygon simple check (ray casting approximation)
    x, y = bbox_center
    n = len(coords)
    inside = False
    
    p1x, p1y = coords[0]
    for i in range(1, n + 1):
        p2x, p2y = coords[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    
    return inside


def create_bbox_polygon(bbox: Tuple[float, float, float, float]) -> List[List[float]]:
    """Crée un polygone à partir d'une bounding box"""
    min_lon, min_lat, max_lon, max_lat = bbox
    return [
        [min_lon, min_lat],
        [max_lon, min_lat],
        [max_lon, max_lat],
        [min_lon, max_lat],
        [min_lon, min_lat]  # Fermer le polygone
    ]


def split_ward_into_subdivisions(ward_feature: dict, subdivisions: int = 3) -> List[dict]:
    """Divise un ward en subdivisions"""
    if ward_feature['geometry']['type'] != 'Polygon':
        print(f"⚠️  Type de géométrie non supporté: {ward_feature['geometry']['type']}")
        return [ward_feature]  # Retourner tel quel
    
    coordinates = ward_feature['geometry']['coordinates']
    
    # Obtenir les limites du ward
    min_lon, min_lat, max_lon, max_lat = get_polygon_bounds(coordinates)
    
    # Créer la grille de subdivision
    cells = create_subdivision_grid(min_lon, min_lat, max_lon, max_lat, subdivisions)
    
    subdivision_features = []
    
    for i, bbox in enumerate(cells):
        # Créer un polygone pour cette subdivision (rectangle)
        subdivision_coords = [create_bbox_polygon(bbox)]
        
        # Créer la feature pour cette subdivision
        subdivision_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": subdivision_coords
            },
            "properties": {
                **ward_feature['properties'],  # Copier toutes les propriétés originales
                "subdivision_id": i + 1,
                "subdivision_total": subdivisions,
                "original_ward": ward_feature['properties'].get('WardNo', 'unknown'),
                "subdivision_bbox": bbox
            }
        }
        
        subdivision_features.append(subdivision_feature)
    
    return subdivision_features


def process_wards_with_subdivision(input_path: str, ward_list: List[int], 
                                 output_directory: str = None, subdivisions: int = 3,
                                 invert_coords: bool = True, single_file: bool = True) -> Dict:
    """Divise les wards spécifiés en subdivisions"""
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier {input_path} n'existe pas")
    
    if output_directory is None:
        output_directory = str(Path(input_path).parent)
    
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    print(f"📖 Lecture du fichier: {input_path}")
    print(f"🎯 Wards à traiter: {ward_list}")
    print(f"✂️  Subdivisions par ward: {subdivisions}")
    print(f"📁 Dossier de sortie: {output_directory}")
    print(f"📄 Fichier unique: {'Oui' if single_file else 'Non'}")
    print(f"🔄 Inversion coordonnées: {'Oui' if invert_coords else 'Non'}")
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print(f"✅ Fichier chargé avec succès")
        
        if geojson_data.get('type') != 'FeatureCollection':
            raise ValueError("Le fichier doit être un FeatureCollection GeoJSON")
        
        ward_features = {}
        total_features = len(geojson_data.get('features', []))
        found_wards = set()
        
        print(f"🔍 Analyse de {total_features} features...")
        
        # Trouver les features pour chaque ward
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
                ward_features[ward_number] = feature
                found_wards.add(ward_number)
        
        stats = {
            'input_file': input_path,
            'output_directory': output_directory,
            'requested_wards': ward_list,
            'found_wards': list(found_wards),
            'missing_wards': [w for w in ward_list if w not in found_wards],
            'subdivisions_per_ward': subdivisions,
            'total_subdivisions_created': 0,
            'subdivision_files_created': [],
            'coordinates_inverted': invert_coords,
            'single_file': single_file,
            'success': True
        }
        
        # Collecter toutes les subdivisions
        all_subdivision_features = []
        total_coordinate_count = 0
        
        # Traiter chaque ward
        for ward_num in ward_list:
            if ward_num not in ward_features:
                print(f"⚠️  Ward {ward_num}: Non trouvé")
                continue
            
            print(f"✂️  Ward {ward_num}: Division en {subdivisions} subdivisions...")
            
            ward_feature = ward_features[ward_num]
            subdivision_features = split_ward_into_subdivisions(ward_feature, subdivisions)
            
            # Ajouter chaque subdivision à la liste globale
            for i, subdivision_feature in enumerate(subdivision_features):
                all_subdivision_features.append(subdivision_feature)
                stats['total_subdivisions_created'] += 1
                
                subdivision_stats = {
                    'ward_number': ward_num,
                    'subdivision_id': i + 1,
                    'coordinates_processed': 0,  # Sera mis à jour après inversion
                }
                stats['subdivision_files_created'].append(subdivision_stats)
                
                print(f"✅ Ward {ward_num} Sub {i+1}: Ajouté à la collection")
        
        if single_file and all_subdivision_features:
            # Créer un seul GeoJSON avec toutes les subdivisions
            combined_geojson = {
                "type": "FeatureCollection",
                "crs": geojson_data.get("crs"),
                "features": all_subdivision_features
            }
            
            # Inverser les coordonnées si demandé
            if invert_coords:
                print(f"🔄 Inversion des coordonnées pour toutes les subdivisions...")
                combined_geojson, total_coordinate_count = process_geojson_coordinates(combined_geojson)
            
            # Définir le nom du fichier unique
            input_name = Path(input_path).stem
            coord_suffix = "_inv" if invert_coords else ""
            output_filename = f"{input_name}_ward_subdivisions{coord_suffix}.geojson"
            output_path = os.path.join(output_directory, output_filename)
            
            # Écrire le fichier unique
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(combined_geojson, f, indent=2, ensure_ascii=False)
            
            stats['output_file'] = output_path
            print(f"✅ Fichier unique créé: {output_filename}")
            if invert_coords:
                print(f"   📍 Total coordonnées inversées: {total_coordinate_count:,}")
        
        elif not single_file:
            # Mode fichiers séparés (code original)
            for i, subdivision_feature in enumerate(all_subdivision_features):
                subdivision_geojson = {
                    "type": "FeatureCollection",
                    "crs": geojson_data.get("crs"),
                    "features": [subdivision_feature]
                }
                
                # Inverser les coordonnées si demandé
                coordinate_count = 0
                if invert_coords:
                    subdivision_geojson, coordinate_count = process_geojson_coordinates(subdivision_geojson)
                
                # Définir le nom du fichier
                ward_num = subdivision_feature['properties']['original_ward']
                sub_id = subdivision_feature['properties']['subdivision_id']
                input_name = Path(input_path).stem
                coord_suffix = "_inv" if invert_coords else ""
                output_filename = f"{input_name}_ward_{ward_num}_sub_{sub_id}{coord_suffix}.geojson"
                output_path = os.path.join(output_directory, output_filename)
                
                # Écrire le fichier
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(subdivision_geojson, f, indent=2, ensure_ascii=False)
                
                print(f"✅ Fichier créé: {output_filename}")
                if invert_coords:
                    print(f"   📍 Coordonnées inversées: {coordinate_count:,}")
        
        print(f"\n🎉 Traitement terminé!")
        print(f"   📊 Wards demandés: {len(ward_list)}")
        print(f"   ✅ Wards trouvés: {len(found_wards)}")
        print(f"   ✂️  Subdivisions créées: {stats['total_subdivisions_created']}")
        if single_file:
            print(f"   📄 Fichier unique: {stats.get('output_file', 'N/A')}")
        else:
            print(f"   📁 Fichiers créés: {len(stats['subdivision_files_created'])}")
        
        if stats['missing_wards']:
            print(f"   ⚠️  Wards non trouvés: {stats['missing_wards']}")
        
        return stats
        
    except Exception as e:
        error_msg = f"Erreur lors du traitement: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}


def main():
    parser = argparse.ArgumentParser(
        description="Divise chaque ward en subdivisions avec inversion optionnelle des coordonnées",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:

  # Diviser les wards en 3 subdivisions chacun
  python %(prog)s -i johannesburg.geojson -w 25,89,93,97,112,120,124,125,132 --subdivisions 3

  # Diviser en 4 subdivisions avec dossier de sortie spécifique
  python %(prog)s -i johannesburg.geojson -w 25,89,93 --subdivisions 4 -o ./ward_subdivisions

  # Sans inversion de coordonnées
  python %(prog)s -i johannesburg.geojson -w 25,89,93 --subdivisions 3 --no-invert
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
        help='Liste des wards à traiter (ex: 25,89,93,97,112,120,124,125,132)'
    )
    
    parser.add_argument(
        '--subdivisions',
        type=int,
        default=3,
        help='Nombre de subdivisions par ward (défaut: 3)'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Dossier de sortie (par défaut: même dossier que l\'entrée)'
    )
    
    parser.add_argument(
        '--invert',
        action='store_true',
        default=True,
        help='Inverse les coordonnées [lon,lat] -> [lat,lon] (défaut)'
    )
    
    parser.add_argument(
        '--no-invert',
        action='store_true',
        help='Ne pas inverser les coordonnées'
    )
    
    parser.add_argument(
        '--single-file',
        action='store_true',
        default=True,
        help='Enregistrer toutes les subdivisions dans un seul fichier (défaut)'
    )
    
    parser.add_argument(
        '--separate-files',
        action='store_true',
        help='Créer un fichier séparé pour chaque subdivision'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Mode verbeux'
    )
    
    args = parser.parse_args()
    
    # Parser la liste des wards
    try:
        ward_list = [int(w.strip()) for w in args.wards.split(',') if w.strip()]
    except ValueError:
        print("❌ Erreur: Format de wards invalide. Utilisez: 25,89,93,97")
        sys.exit(1)
    
    # Gestion de l'inversion des coordonnées
    invert_coords = args.invert and not args.no_invert
    
    # Gestion du fichier unique vs fichiers séparés
    single_file = args.single_file and not args.separate_files
    
    if args.verbose:
        print(f"🚀 Démarrage de la subdivision des wards")
        print(f"📂 Fichier d'entrée: {args.input}")
        print(f"🎯 Wards: {ward_list}")
        print(f"✂️  Subdivisions: {args.subdivisions}")
        print(f"📁 Sortie: {args.output or 'Même dossier'}")
        print(f"📄 Fichier unique: {'Oui' if single_file else 'Non'}")
        print(f"🔄 Inversion: {'Oui' if invert_coords else 'Non'}")
        print("-" * 50)
    
    try:
        stats = process_wards_with_subdivision(
            input_path=args.input,
            ward_list=ward_list,
            output_directory=args.output,
            subdivisions=args.subdivisions,
            invert_coords=invert_coords,
            single_file=single_file
        )
        
        if stats['success']:
            print(f"\n🌟 Subdivision réussie!")
            print(f"📊 {stats['total_subdivisions_created']} subdivisions créées pour {len(stats['found_wards'])} wards")
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