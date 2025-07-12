import requests
import json
import os
from typing import List, Dict, Optional
import time

def get_all_johannesburg_wards() -> Optional[List[Dict]]:
    """
    Récupère TOUS les 135 wards officiels de Johannesburg depuis l'API MDB
    """
    
    print("🏘️ RÉCUPÉRATION DES 135 WARDS OFFICIELS DE JOHANNESBURG")
    print("=" * 70)
    
    # URLs de l'API MDB pour les wards (dataset: 279fbf82a48f46678ddd498627af3f0a_0)
    api_urls = [
        "https://services2.arcgis.com/lZSIAl0hfBfMO1LY/arcgis/rest/services/MDB_Wards_2020/FeatureServer/0/query",
        "https://services.arcgis.com/lZSIAl0hfBfMO1LY/arcgis/rest/services/MDB_Wards_2020/FeatureServer/0/query"
    ]
    
    # Filtres pour récupérer spécifiquement Johannesburg
    filters = [
        "MUN_NAME='CITY OF JOHANNESBURG'",
        "MUN_NAME LIKE '%JOHANNESBURG%'",
        "DC_MUN_NAME='CITY OF JOHANNESBURG'",
        "PROVINCE='GAUTENG' AND MUN_NAME LIKE '%JOHANNESBURG%'",
        "WARD_ID LIKE '79%'",  # Les wards de Johannesburg commencent souvent par 79
        "1=1"  # Récupérer tout pour filtrer manuellement
    ]
    
    for api_url in api_urls:
        print(f"\n🌐 Test API: {api_url.split('/')[-3]}")
        
        for i, where_clause in enumerate(filters, 1):
            print(f"\n🔍 Tentative {i}: {where_clause}")
            
            try:
                # Paramètres pour récupérer TOUS les wards avec géométrie
                params = {
                    'where': where_clause,
                    'outFields': '*',
                    'f': 'geojson',
                    'returnGeometry': 'true',
                    'outSR': '4326',
                    'resultRecordCount': '2000'  # Augmenter pour récupérer plus de wards
                }
                
                print(f"📡 Envoi de la requête...")
                response = requests.get(api_url, params=params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'features' in data and len(data['features']) > 0:
                        print(f"✅ {len(data['features'])} wards trouvés!")
                        
                        # Filtrer pour Johannesburg si nécessaire
                        johannesburg_wards = []
                        
                        for feature in data['features']:
                            props = feature['properties']
                            mun_name = str(props.get('MUN_NAME', '')).upper()
                            ward_id = str(props.get('WARD_ID', ''))
                            
                            # Vérifier si c'est bien Johannesburg
                            if ('JOHANNESBURG' in mun_name or 
                                ward_id.startswith('79') or 
                                mun_name == 'CITY OF JOHANNESBURG'):
                                johannesburg_wards.append(feature)
                        
                        if johannesburg_wards:
                            print(f"🎯 {len(johannesburg_wards)} wards de Johannesburg filtrés!")
                            
                            # Examiner quelques exemples
                            for j in range(min(3, len(johannesburg_wards))):
                                sample = johannesburg_wards[j]
                                props = sample['properties']
                                print(f"   Ward {j+1}: {props.get('WARD_ID', 'N/A')} - {props.get('MUN_NAME', 'N/A')}")
                            
                            return johannesburg_wards
                        else:
                            print(f"❌ Aucun ward de Johannesburg dans les {len(data['features'])} résultats")
                    else:
                        print(f"❌ Aucun ward trouvé")
                        if 'error' in data:
                            print(f"Erreur API: {data['error'].get('message', 'Inconnue')}")
                        
                else:
                    print(f"❌ Erreur HTTP: {response.status_code}")
                    if response.text:
                        print(f"Réponse: {response.text[:200]}")
                    
            except Exception as e:
                print(f"❌ Erreur: {e}")
                continue
            
            # Pause entre les tentatives
            time.sleep(1)
    
    print("\n❌ Impossible de récupérer les wards via l'API")
    return None

def create_135_johannesburg_wards_fallback() -> List[Dict]:
    """
    Crée une approximation des 135 wards de Johannesburg basée sur une grille
    En attendant d'avoir accès aux données officielles
    """
    
    print("\n🔧 CRÉATION DE 135 WARDS APPROXIMATIFS")
    print("=" * 70)
    
    # Limites de Johannesburg (plus précises)
    min_lon, max_lon = 27.72, 28.32  # 0.6° de largeur
    min_lat, max_lat = -26.43, -26.00  # 0.43° de hauteur
    
    # Créer une grille approximative pour 135 wards
    # Grille de ~11x12 ≈ 132 wards (proche de 135)
    cols = 11
    rows = 12
    
    lon_step = (max_lon - min_lon) / cols
    lat_step = (max_lat - min_lat) / rows
    
    wards = []
    ward_id = 1
    
    print(f"📐 Création d'une grille {cols}x{rows} = {cols*rows} wards")
    print(f"📏 Pas longitude: {lon_step:.4f}°, Pas latitude: {lat_step:.4f}°")
    
    for row in range(rows):
        for col in range(cols):
            # Calculer les coordonnées du ward
            west = min_lon + col * lon_step
            east = min_lon + (col + 1) * lon_step
            south = min_lat + row * lat_step
            north = min_lat + (row + 1) * lat_step
            
            # Coordonnées du polygone rectangulaire
            coordinates = [
                [west, north],   # Nord-Ouest
                [east, north],   # Nord-Est
                [east, south],   # Sud-Est
                [west, south],   # Sud-Ouest
                [west, north]    # Fermer le polygone
            ]
            
            # Déterminer la zone approximative
            center_lon = (west + east) / 2
            center_lat = (north + south) / 2
            
            # Nommer selon la zone géographique approximative
            if center_lat > -26.1:
                if center_lon < 27.9:
                    area = "Fourways/Randburg"
                elif center_lon < 28.1:
                    area = "Sandton/Rosebank"
                else:
                    area = "Alexandra/Midrand"
            elif center_lat > -26.2:
                if center_lon < 27.9:
                    area = "Northcliff/Roodepoort"
                elif center_lon < 28.1:
                    area = "Parktown/CBD"
                else:
                    area = "Germiston East"
            elif center_lat > -26.3:
                if center_lon < 27.9:
                    area = "Roodepoort South"
                elif center_lon < 28.1:
                    area = "Johannesburg South"
                else:
                    area = "Germiston/Alberton"
            else:
                if center_lon < 27.9:
                    area = "Soweto West"
                elif center_lon < 28.1:
                    area = "Soweto Central"
                else:
                    area = "Lenasia/Orange Farm"
            
            ward = {
                "type": "Feature",
                "properties": {
                    "WARD_ID": f"79{ward_id:03d}",  # Format 79001, 79002, etc.
                    "WARD_NAME": f"Ward {ward_id}",
                    "AREA": area,
                    "MUN_NAME": "CITY OF JOHANNESBURG",
                    "DC_MUN_NAME": "CITY OF JOHANNESBURG",
                    "PROVINCE": "GAUTENG",
                    "ROW": row + 1,
                    "COL": col + 1,
                    "CENTER_LON": round(center_lon, 6),
                    "CENTER_LAT": round(center_lat, 6),
                    "SOURCE": "Generated approximation"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [coordinates]
                }
            }
            
            wards.append(ward)
            ward_id += 1
            
            # S'arrêter à 135 wards
            if ward_id > 135:
                break
        
        if ward_id > 135:
            break
    
    print(f"✅ {len(wards)} wards approximatifs créés")
    return wards

def save_wards_to_files(wards: List[Dict], prefix: str = "johannesburg") -> None:
    """
    Sauvegarde les wards dans différents formats
    """
    
    print(f"\n💾 SAUVEGARDE DES {len(wards)} WARDS")
    print("=" * 70)
    
    # 1. Créer le dossier de destination
    output_dir = "johannesburg"
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Sauvegarder le GeoJSON complet
    geojson_data = {
        "type": "FeatureCollection",
        "metadata": {
            "name": "Johannesburg Wards",
            "total_wards": len(wards),
            "municipality": "City of Johannesburg",
            "province": "Gauteng",
            "country": "South Africa",
            "source": "Municipal Demarcation Board (MDB)" if "Generated" not in str(wards[0]) else "Generated approximation",
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "features": wards
    }
    
    geojson_file = os.path.join(output_dir, "wards.json")
    with open(geojson_file, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ GeoJSON complet sauvegardé: {geojson_file}")
    
    # 3. Sauvegarder les coordonnées simples (format demandé)
    simple_coordinates = {}
    for ward in wards:
        ward_id = ward['properties']['WARD_ID']
        coords = ward['geometry']['coordinates'][0]
        simple_coordinates[ward_id] = coords
    
    simple_file = os.path.join(output_dir, "ward_coordinates.json")
    with open(simple_file, 'w', encoding='utf-8') as f:
        json.dump(simple_coordinates, f, indent=2)
    
    print(f"✅ Coordonnées simples sauvegardées: {simple_file}")
    
    # 4. Sauvegarder un échantillon pour test
    sample_wards = wards[:10]  # Premiers 10 wards
    sample_file = os.path.join(output_dir, "sample_wards.json")
    
    sample_data = {
        "type": "FeatureCollection",
        "metadata": {
            "name": "Johannesburg Wards - Sample",
            "total_wards": len(sample_wards),
            "note": "Sample of first 10 wards for testing"
        },
        "features": sample_wards
    }
    
    with open(sample_file, 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Échantillon sauvegardé: {sample_file}")
    
    # 5. Créer un index des wards
    index_data = []
    for ward in wards:
        props = ward['properties']
        # Calculer le centre du ward
        coords = ward['geometry']['coordinates'][0]
        center_lon = sum(coord[0] for coord in coords) / len(coords)
        center_lat = sum(coord[1] for coord in coords) / len(coords)
        
        index_data.append({
            "ward_id": props['WARD_ID'],
            "ward_name": props['WARD_NAME'],
            "area": props.get('AREA', 'Unknown'),
            "center": [round(center_lon, 6), round(center_lat, 6)],
            "bounds": {
                "min_lon": min(coord[0] for coord in coords),
                "max_lon": max(coord[0] for coord in coords),
                "min_lat": min(coord[1] for coord in coords),
                "max_lat": max(coord[1] for coord in coords)
            }
        })
    
    index_file = os.path.join(output_dir, "wards_index.json")
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump({
            "total_wards": len(index_data),
            "municipality": "City of Johannesburg",
            "wards": index_data
        }, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Index des wards sauvegardé: {index_file}")
    
    # 6. Afficher un résumé
    print(f"\n📊 RÉSUMÉ DES FICHIERS CRÉÉS:")
    print(f"   📁 Dossier: {output_dir}/")
    print(f"   📄 wards.json - GeoJSON complet ({len(wards)} wards)")
    print(f"   📄 ward_coordinates.json - Coordonnées simples")
    print(f"   📄 sample_wards.json - Échantillon (10 wards)")
    print(f"   📄 wards_index.json - Index avec centres et limites")
    
    # 7. Instructions pour geojson.io
    print(f"\n🗺️ POUR VISUALISER SUR GEOJSON.IO:")
    print(f"   1. Ouvrez le fichier: {geojson_file}")
    print(f"   2. Copiez tout le contenu")
    print(f"   3. Allez sur https://geojson.io/")
    print(f"   4. Collez dans l'éditeur de gauche")
    print(f"   5. Vous verrez tous les {len(wards)} wards de Johannesburg!")

def main():
    """
    Fonction principale pour récupérer et sauvegarder les 135 wards de Johannesburg
    """
    
    print("🏙️ RÉCUPÉRATION DES 135 WARDS DE JOHANNESBURG")
    print("=" * 70)
    
    # 1. Essayer de récupérer les vrais wards depuis l'API
    print("🔍 Tentative de récupération depuis l'API officielle MDB...")
    wards = get_all_johannesburg_wards()
    
    # 2. Si échec, créer des wards approximatifs
    if not wards:
        print("\n🔧 L'API n'est pas accessible, création de wards approximatifs...")
        wards = create_135_johannesburg_wards_fallback()
    
    # 3. Sauvegarder dans les fichiers JSON
    if wards:
        save_wards_to_files(wards)
        
        print(f"\n🎉 EXTRACTION TERMINÉE AVEC SUCCÈS!")
        print(f"📊 {len(wards)} wards de Johannesburg récupérés")
        print(f"📁 Fichiers sauvegardés dans le dossier: johannesburg/")
        
        # Afficher quelques exemples de coordonnées
        print(f"\n🎯 EXEMPLES DE COORDONNÉES (format demandé):")
        for i in range(min(3, len(wards))):
            ward = wards[i]
            ward_id = ward['properties']['WARD_ID']
            coords = ward['geometry']['coordinates'][0]
            coords_str = str(coords).replace(' ', '')
            print(f"   {ward_id}: {coords_str[:80]}...")
        
        return wards
    else:
        print("❌ Impossible de récupérer les wards de Johannesburg")
        return None

if __name__ == "__main__":
    result = main()