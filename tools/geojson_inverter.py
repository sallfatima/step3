import json
import os
from pathlib import Path
from typing import List, Union, Any


def invert_coordinates(coordinates: Union[List, tuple]) -> List:
    """
    Inverse récursivement les coordonnées [lon, lat] vers [lat, lon]
    
    Args:
        coordinates: Coordonnées à inverser (peut être une liste imbriquée)
        
    Returns:
        Coordonnées inversées
    """
    if isinstance(coordinates[0], (list, tuple)):
        # Si c'est une liste de coordonnées, traiter récursivement
        return [invert_coordinates(coord) for coord in coordinates]
    else:
        # Si c'est une paire de coordonnées [lon, lat], inverser vers [lat, lon]
        return [coordinates[1], coordinates[0]]


def process_geojson_coordinates(geojson_data: dict) -> dict:
    """
    Traite un objet GeoJSON et inverse toutes les coordonnées
    
    Args:
        geojson_data: Dictionnaire contenant les données GeoJSON
        
    Returns:
        GeoJSON avec coordonnées inversées
    """
    # Créer une copie profonde pour éviter de modifier l'original
    import copy
    result = copy.deepcopy(geojson_data)
    
    coordinate_count = 0
    
    if result.get('type') == 'FeatureCollection':
        for feature in result.get('features', []):
            if 'geometry' in feature and feature['geometry'] is not None:
                geometry = feature['geometry']
                if 'coordinates' in geometry and geometry['coordinates'] is not None:
                    # Inverser les coordonnées
                    geometry['coordinates'] = invert_coordinates(geometry['coordinates'])
                    
                    # Compter approximativement les coordonnées traitées
                    coord_str = json.dumps(geometry['coordinates'])
                    coordinate_count += coord_str.count(',') // 2
    
    elif result.get('type') == 'Feature':
        # Si c'est une Feature unique
        if 'geometry' in result and result['geometry'] is not None:
            geometry = result['geometry']
            if 'coordinates' in geometry and geometry['coordinates'] is not None:
                geometry['coordinates'] = invert_coordinates(geometry['coordinates'])
    
    elif result.get('type') in ['Point', 'LineString', 'Polygon', 'MultiPoint', 
                                'MultiLineString', 'MultiPolygon']:
        # Si c'est une géométrie directe
        if 'coordinates' in result and result['coordinates'] is not None:
            result['coordinates'] = invert_coordinates(result['coordinates'])
    
    return result, coordinate_count


def invert_geojson_file(input_path: str, output_path: str = None) -> dict:
    """
    Lit un fichier GeoJSON et crée une version avec les coordonnées inversées
    
    Args:
        input_path: Chemin vers le fichier GeoJSON d'entrée
        output_path: Chemin vers le fichier de sortie (optionnel)
        
    Returns:
        Dictionnaire avec les statistiques du traitement
    """
    
    # Validation du fichier d'entrée
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier {input_path} n'existe pas")
    
    # Générer le chemin de sortie si non fourni
    if output_path is None:
        input_path_obj = Path(input_path)
        output_path = str(input_path_obj.parent / f"{input_path_obj.stem}_inv{input_path_obj.suffix}")
    
    # Créer le dossier de sortie si nécessaire
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print(f"📖 Lecture du fichier: {input_path}")
    
    try:
        # Lire le fichier GeoJSON
        with open(input_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print(f"✅ Fichier chargé avec succès")
        
        # Traiter les coordonnées
        print(f"🔄 Inversion des coordonnées...")
        processed_data, coordinate_count = process_geojson_coordinates(geojson_data)
        
        # Écrire le fichier de sortie
        print(f"💾 Écriture du fichier: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
        
        # Statistiques
        feature_count = 0
        if processed_data.get('type') == 'FeatureCollection':
            feature_count = len(processed_data.get('features', []))
        elif processed_data.get('type') == 'Feature':
            feature_count = 1
        
        stats = {
            'input_file': input_path,
            'output_file': output_path,
            'feature_count': feature_count,
            'coordinate_count': coordinate_count,
            'success': True
        }
        
        print(f"✅ Traitement terminé!")
        print(f"   📊 Features traitées: {feature_count}")
        print(f"   📍 Coordonnées inversées: {coordinate_count:,}")
        print(f"   📁 Fichier de sortie: {output_path}")
        
        return stats
        
    except json.JSONDecodeError as e:
        error_msg = f"Erreur de format JSON: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}
    
    except Exception as e:
        error_msg = f"Erreur lors du traitement: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}


def main():
    """
    Fonction principale pour traiter le fichier de Johannesburg
    """
    input_file = "johannesburg/johannesburg.geojson"
    output_file = "johannesburg/johannesburg_inv.geojson"
    
    try:
        stats = invert_geojson_file(input_file, output_file)
        
        if stats['success']:
            print("\n🎉 Conversion réussie!")
            print(f"Le fichier avec coordonnées inversées a été créé: {output_file}")
        else:
            print(f"\n❌ Échec de la conversion: {stats['error']}")
            
    except Exception as e:
        print(f"\n❌ Erreur: {str(e)}")


# Fonction utilitaire pour traiter plusieurs fichiers
def batch_invert_geojson(input_directory: str, output_directory: str = None):
    """
    Traite tous les fichiers GeoJSON dans un dossier
    
    Args:
        input_directory: Dossier contenant les fichiers GeoJSON
        output_directory: Dossier de sortie (optionnel)
    """
    if output_directory is None:
        output_directory = input_directory
    
    geojson_files = list(Path(input_directory).glob("*.geojson"))
    
    if not geojson_files:
        print(f"Aucun fichier .geojson trouvé dans {input_directory}")
        return
    
    print(f"🔍 {len(geojson_files)} fichier(s) GeoJSON trouvé(s)")
    
    for geojson_file in geojson_files:
        output_file = Path(output_directory) / f"{geojson_file.stem}_inv{geojson_file.suffix}"
        
        print(f"\n📁 Traitement de: {geojson_file}")
        stats = invert_geojson_file(str(geojson_file), str(output_file))
        
        if not stats['success']:
            print(f"❌ Échec pour {geojson_file}: {stats['error']}")


if __name__ == "__main__":
    main()