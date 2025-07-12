"""
Processeur de Wards pour Johannesburg
Lit johannesburg_inv.geojson, détecte automatiquement les wards et fait le split
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Set


def read_johannesburg_geojson(file_path: str = "johannesburg/johannesburg_inv.geojson") -> Dict:
    """
    Lit le fichier GeoJSON de Johannesburg
    
    Args:
        file_path: Chemin vers le fichier GeoJSON
        
    Returns:
        Données GeoJSON parsées
    """
    print(f"📖 Lecture du fichier: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Le fichier {file_path} n'existe pas")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        print(f"✅ Fichier chargé avec succès")
        return geojson_data
        
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ Erreur de format JSON: {str(e)}")
    except Exception as e:
        raise Exception(f"❌ Erreur lors de la lecture: {str(e)}")


def detect_wards_in_geojson(geojson_data: Dict) -> Dict:
    """
    Détecte tous les wards présents dans le GeoJSON
    
    Args:
        geojson_data: Données GeoJSON
        
    Returns:
        Dictionnaire avec les informations sur les wards détectés
    """
    print(f"🔍 Détection des wards...")
    
    if geojson_data.get('type') != 'FeatureCollection':
        raise ValueError("❌ Le fichier doit être un FeatureCollection GeoJSON")
    
    ward_info = {}
    ward_counts = {}
    total_features = len(geojson_data.get('features', []))
    features_with_ward = 0
    
    # Champs possibles pour les wards
    possible_ward_fields = [
        'WardNo', 'Ward', 'ward_no', 'ward', 'WARD_NO', 'WARD',
        'WardNumber', 'ward_number', 'WARD_NUMBER', 'wardno'
    ]
    
    print(f"   📊 Analyse de {total_features} features...")
    
    for i, feature in enumerate(geojson_data.get('features', [])):
        properties = feature.get('properties', {})
        ward_number = None
        ward_field_used = None
        
        # Chercher le champ contenant le ward
        for field in possible_ward_fields:
            if field in properties:
                try:
                    ward_number = int(properties[field])
                    ward_field_used = field
                    break
                except (ValueError, TypeError):
                    continue
        
        if ward_number is not None:
            features_with_ward += 1
            
            # Compter les features par ward
            ward_counts[ward_number] = ward_counts.get(ward_number, 0) + 1
            
            # Stocker des infos détaillées pour chaque ward
            if ward_number not in ward_info:
                ward_info[ward_number] = {
                    'ward_number': ward_number,
                    'field_name': ward_field_used,
                    'features': [],
                    'sample_properties': properties
                }
            
            ward_info[ward_number]['features'].append(i)
    
    # Créer le résumé
    ward_analysis = {
        'total_features': total_features,
        'features_with_ward': features_with_ward,
        'features_without_ward': total_features - features_with_ward,
        'unique_wards': sorted(ward_counts.keys()),
        'ward_counts': ward_counts,
        'ward_details': ward_info,
        'min_ward': min(ward_counts.keys()) if ward_counts else None,
        'max_ward': max(ward_counts.keys()) if ward_counts else None,
        'ward_field_detected': ward_info[list(ward_info.keys())[0]]['field_name'] if ward_info else None
    }
    
    print(f"✅ Détection terminée:")
    print(f"   • Total features: {total_features}")
    print(f"   • Features avec ward: {features_with_ward}")
    print(f"   • Features sans ward: {ward_analysis['features_without_ward']}")
    print(f"   • Wards uniques: {len(ward_analysis['unique_wards'])}")
    print(f"   • Range des wards: {ward_analysis['min_ward']} - {ward_analysis['max_ward']}")
    print(f"   • Champ ward détecté: '{ward_analysis['ward_field_detected']}'")
    
    if ward_analysis['features_without_ward'] > 0:
        print(f"   ⚠️  {ward_analysis['features_without_ward']} features sans numéro de ward")
    
    return ward_analysis


def split_wards(geojson_data: Dict, ward_analysis: Dict, 
                target_wards: List[int] = None, 
                output_dir: str = "johannesburg/wards") -> Dict:
    """
    Divise le GeoJSON en fichiers séparés par ward
    
    Args:
        geojson_data: Données GeoJSON originales
        ward_analysis: Résultats de l'analyse des wards
        target_wards: Liste des wards à extraire (None = tous)
        output_dir: Dossier de sortie
        
    Returns:
        Statistiques du split
    """
    
    # Déterminer quels wards traiter
    if target_wards is None:
        wards_to_process = ward_analysis['unique_wards']
        print(f"🎯 Split de TOUS les wards détectés: {len(wards_to_process)} wards")
    else:
        wards_to_process = [w for w in target_wards if w in ward_analysis['unique_wards']]
        missing_wards = [w for w in target_wards if w not in ward_analysis['unique_wards']]
        print(f"🎯 Split des wards spécifiés: {wards_to_process}")
        if missing_wards:
            print(f"   ⚠️  Wards demandés mais non trouvés: {missing_wards}")
    
    # Créer le dossier de sortie
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Dossier de sortie: {output_dir}")
    
    # Conserver le CRS original
    original_crs = geojson_data.get("crs")
    all_features = geojson_data.get('features', [])
    
    split_results = {
        'output_directory': output_dir,
        'processed_wards': [],
        'skipped_wards': [],
        'total_output_features': 0,
        'files_created': []
    }
    
    # Traiter chaque ward
    for ward_num in wards_to_process:
        ward_details = ward_analysis['ward_details'][ward_num]
        feature_indices = ward_details['features']
        
        print(f"📝 Ward {ward_num}: {len(feature_indices)} feature(s)")
        
        # Extraire les features pour ce ward
        ward_features = [all_features[i] for i in feature_indices]
        
        # Créer le GeoJSON pour ce ward
        ward_geojson = {
            "type": "FeatureCollection",
            "crs": original_crs,
            "features": ward_features
        }
        
        # Nom du fichier de sortie
        output_filename = f"johannesburg_ward_{ward_num}_inv.geojson"
        output_path = os.path.join(output_dir, output_filename)
        
        # Écrire le fichier
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(ward_geojson, f, indent=2, ensure_ascii=False)
            
            # Ajouter aux résultats
            ward_result = {
                'ward_number': ward_num,
                'features_count': len(ward_features),
                'output_file': output_path,
                'filename': output_filename
            }
            split_results['processed_wards'].append(ward_result)
            split_results['total_output_features'] += len(ward_features)
            split_results['files_created'].append(output_filename)
            
            print(f"✅ Ward {ward_num}: {output_filename} créé")
            
        except Exception as e:
            print(f"❌ Erreur pour Ward {ward_num}: {str(e)}")
            split_results['skipped_wards'].append({
                'ward_number': ward_num,
                'error': str(e)
            })
    
    return split_results


def process_johannesburg_wards(
    input_file: str = "johannesburg/johannesburg_inv.geojson",
    ward_list: List[int] = None,
    output_dir: str = "johannesburg/wards"
) -> Dict:
    """
    Fonction principale pour traiter les wards de Johannesburg
    
    Args:
        input_file: Fichier GeoJSON d'entrée
        ward_list: Liste des wards à traiter (None = tous)
        output_dir: Dossier de sortie
        
    Returns:
        Résultats complets du traitement
    """
    
    print("🚀 === TRAITEMENT DES WARDS DE JOHANNESBURG ===\n")
    
    try:
        # 1. Lire le fichier GeoJSON
        geojson_data = read_johannesburg_geojson(input_file)
        
        # 2. Détecter les wards
        print(f"\n" + "="*50)
        ward_analysis = detect_wards_in_geojson(geojson_data)
        
        # 3. Afficher un résumé des wards disponibles
        print(f"\n📋 WARDS DISPONIBLES:")
        wards_preview = ward_analysis['unique_wards'][:20]  # Afficher les 20 premiers
        print(f"   Premiers wards: {wards_preview}")
        if len(ward_analysis['unique_wards']) > 20:
            print(f"   ... et {len(ward_analysis['unique_wards']) - 20} autres")
        
        # 4. Faire le split
        print(f"\n" + "="*50)
        split_results = split_wards(geojson_data, ward_analysis, ward_list, output_dir)
        
        # 5. Résumé final
        print(f"\n🎉 === TRAITEMENT TERMINÉ ===")
        print(f"   📊 Wards traités: {len(split_results['processed_wards'])}")
        print(f"   📁 Fichiers créés: {len(split_results['files_created'])}")
        print(f"   📍 Total features: {split_results['total_output_features']}")
        
        if split_results['skipped_wards']:
            print(f"   ⚠️  Wards échoués: {len(split_results['skipped_wards'])}")
        
        # Retourner toutes les informations
        return {
            'success': True,
            'input_file': input_file,
            'ward_analysis': ward_analysis,
            'split_results': split_results
        }
        
    except Exception as e:
        error_msg = f"Erreur lors du traitement: {str(e)}"
        print(f"❌ {error_msg}")
        return {
            'success': False,
            'error': error_msg
        }


def show_ward_preview(input_file: str = "johannesburg/johannesburg_inv.geojson", 
                     limit: int = 50) -> List[int]:
    """
    Fonction utilitaire pour voir les wards disponibles
    
    Args:
        input_file: Fichier à analyser
        limit: Nombre maximum de wards à afficher
        
    Returns:
        Liste des wards disponibles
    """
    print(f"👀 === APERÇU DES WARDS DISPONIBLES ===\n")
    
    try:
        geojson_data = read_johannesburg_geojson(input_file)
        ward_analysis = detect_wards_in_geojson(geojson_data)
        
        wards = ward_analysis['unique_wards']
        
        print(f"\n📋 LISTE DES WARDS ({len(wards)} total):")
        
        # Afficher par groupes de 10
        for i in range(0, min(len(wards), limit), 10):
            group = wards[i:i+10]
            print(f"   {group}")
        
        if len(wards) > limit:
            print(f"   ... et {len(wards) - limit} autres wards")
        
        print(f"\n💡 Pour extraire des wards spécifiques, utilisez:")
        print(f"   process_johannesburg_wards(ward_list=[1, 2, 3, 5, 10])")
        
        return wards
        
    except Exception as e:
        print(f"❌ Erreur: {str(e)}")
        return []


def main():
    """
    Fonction principale d'exemple
    """
    
    # Option 1: Voir tous les wards disponibles
    print("=== OPTION 1: APERÇU DES WARDS ===")
    available_wards = show_ward_preview()
    
    if available_wards:
        # Option 2: Traiter quelques wards spécifiques
        print(f"\n" + "="*60)
        print("=== OPTION 2: EXTRACTION DE WARDS SPÉCIFIQUES ===")
        
        # Prendre les 5 premiers wards comme exemple
        sample_wards = available_wards[:5]
        print(f"Exemple avec les wards: {sample_wards}")
        
        results = process_johannesburg_wards(ward_list=sample_wards)
        
        if results['success']:
            print(f"\n✨ Fichiers créés:")
            for file in results['split_results']['files_created']:
                print(f"   • {file}")
    
    # Option 3: Traiter TOUS les wards (décommenter si nécessaire)
    # print(f"\n" + "="*60)
    # print("=== OPTION 3: EXTRACTION DE TOUS LES WARDS ===")
    # results_all = process_johannesburg_wards()  # Sans ward_list = tous les wards


if __name__ == "__main__":
    main()