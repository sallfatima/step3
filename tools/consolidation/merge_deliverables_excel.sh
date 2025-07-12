#!/bin/bash

# ========================================
# CONSOLIDATION DES FICHIERS EXCEL
# ========================================

BUCKET="lengo-geomapping"
SOURCE_BASE="database/africa/south_africa/johannesburg"
DEST_PATH="database/africa/south_africa/johannesburg/johannesburg_custom_all"
# Couleurs
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  CONSOLIDATION DES FICHIERS EXCEL${NC}"
echo -e "${YELLOW}========================================${NC}"

# Créer les dossiers temporaires
mkdir -p temp/excel/{download,processed}

# ========================================
# 1. VÉRIFIER LES DÉPENDANCES
# ========================================
echo -e "\n${BLUE}1. Vérification des dépendances...${NC}"

# Vérifier Python et pandas
if python3 -c "import pandas, openpyxl" &>/dev/null; then
    PANDAS_AVAILABLE=true
    echo -e "${GREEN}✓ Python avec pandas et openpyxl disponible${NC}"
else
    PANDAS_AVAILABLE=false
    echo -e "${YELLOW}⚠ Pandas/openpyxl non disponible${NC}"
    
    # Sur macOS, proposer différentes options
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo ""
        echo "Options pour installer pandas et openpyxl :"
        echo ""
        echo "1. Avec conda (recommandé si vous avez conda) :"
        echo "   conda install pandas openpyxl -y"
        echo ""
        echo "2. Avec pip et --user :"
        echo "   python3 -m pip install --user pandas openpyxl"
        echo ""
        echo "3. Avec pipx :"
        echo "   brew install pipx"
        echo "   pipx install pandas openpyxl"
        echo ""
        echo "4. Dans un environnement virtuel :"
        echo "   python3 -m venv venv"
        echo "   source venv/bin/activate"
        echo "   pip install pandas openpyxl"
        echo ""
        
        # Vérifier si on est dans un environnement conda
        if [ ! -z "$CONDA_DEFAULT_ENV" ]; then
            echo -e "${YELLOW}Vous êtes dans l'environnement conda : $CONDA_DEFAULT_ENV${NC}"
            echo "Tentative d'installation avec conda..."
            
            if conda install pandas openpyxl -y &>/dev/null; then
                PANDAS_AVAILABLE=true
                echo -e "${GREEN}✓ Dépendances installées avec conda${NC}"
            else
                echo -e "${RED}✗ Échec de l'installation avec conda${NC}"
            fi
        else
            # Essayer avec pip --user
            echo "Tentative d'installation avec pip --user..."
            if python3 -m pip install --user pandas openpyxl &>/dev/null; then
                PANDAS_AVAILABLE=true
                echo -e "${GREEN}✓ Dépendances installées avec pip --user${NC}"
            else
                echo -e "${RED}✗ Échec de l'installation${NC}"
            fi
        fi
    else
        # Linux
        echo "Installation avec pip3..."
        pip3 install pandas openpyxl || sudo pip3 install pandas openpyxl
    fi
    
    # Revérifier
    if python3 -c "import pandas, openpyxl" &>/dev/null; then
        PANDAS_AVAILABLE=true
        echo -e "${GREEN}✓ Dépendances maintenant disponibles${NC}"
    else
        echo -e "${RED}✗ Impossible d'installer les dépendances${NC}"
        echo ""
        echo "Veuillez installer manuellement pandas et openpyxl, puis relancer le script."
        exit 1
    fi
fi

# ========================================
# 2. TÉLÉCHARGER TOUS LES FICHIERS EXCEL
# ========================================
echo -e "\n${BLUE}2. Téléchargement des fichiers Excel (.xlsx uniquement)...${NC}"

excel_count=0

# Créer un fichier de suivi
> temp/files_list.txt

for i in {1..135}; do
    echo -ne "\rRecherche ward $i/135..."
    
    # Chercher UNIQUEMENT les fichiers Excel (.xlsx)
    excel_files=$(gcloud storage ls "gs://${BUCKET}/${SOURCE_BASE}/johannesburg_custom_ward_${i}/deliverables/*.xlsx" 2>/dev/null || true)
    
    for file in $excel_files; do
        if [ ! -z "$file" ]; then
            filename=$(basename "$file")
            new_filename="ward_${i}_${filename}"
            
            # Télécharger
            gcloud storage cp "$file" "temp/excel/download/${new_filename}" -q
            echo "${new_filename}|${i}|excel" >> temp/files_list.txt
            ((excel_count++))
        fi
    done
done

echo -e "\n${GREEN}✓ Fichiers Excel trouvés : $excel_count${NC}"

if [ $excel_count -eq 0 ]; then
    echo -e "${RED}Aucun fichier Excel trouvé !${NC}"
    exit 1
fi

# ========================================
# 3. ANALYSER LA STRUCTURE DES FICHIERS
# ========================================
echo -e "\n${BLUE}3. Analyse de la structure des fichiers...${NC}"

python3 << 'PYTHON_EOF'
import pandas as pd
import os
import json
from collections import defaultdict

print("Analyse des colonnes dans les fichiers...")

# Dictionnaire pour stocker les structures
structures = defaultdict(list)
sample_data = {}

# Analyser quelques fichiers pour comprendre la structure
files = os.listdir('temp/excel/download')[:10]  # Premiers 10 fichiers

for filename in files:
    filepath = os.path.join('temp/excel/download', filename)
    
    try:
        if filename.endswith('.xlsx'):
            df = pd.read_excel(filepath, nrows=5)
        else:
            df = pd.read_csv(filepath, nrows=5)
        
        # Enregistrer la structure
        columns = list(df.columns)
        structures[str(sorted(columns))].append(filename)
        
        # Garder un échantillon
        if str(sorted(columns)) not in sample_data:
            sample_data[str(sorted(columns))] = {
                'columns': columns,
                'dtypes': df.dtypes.to_dict(),
                'sample': df.head(2).to_dict()
            }
    except Exception as e:
        print(f"Erreur avec {filename}: {e}")

# Afficher les différentes structures trouvées
print(f"\nNombre de structures différentes : {len(structures)}")
for i, (cols, files) in enumerate(structures.items()):
    print(f"\nStructure {i+1} ({len(files)} fichiers):")
    print(f"Colonnes : {sample_data[cols]['columns']}")
    print(f"Exemple de fichiers : {files[:3]}")

# Sauvegarder l'analyse
with open('temp/structure_analysis.json', 'w') as f:
    json.dump({
        'structures_count': len(structures),
        'samples': sample_data
    }, f, indent=2, default=str)
PYTHON_EOF

# ========================================
# 4. FUSIONNER LES FICHIERS
# ========================================
echo -e "\n${BLUE}4. Fusion des fichiers Excel...${NC}"

python3 << 'PYTHON_EOF'
import pandas as pd
import os
import glob
from datetime import datetime

print("Début de la fusion des fichiers Excel...")

# Liste pour stocker les données
all_data = []

# Statistiques
stats = {
    'files_processed': 0,
    'files_failed': 0,
    'total_rows': 0,
    'columns_found': set(),
    'wards_data': {}
}

# Lire tous les fichiers Excel
files = [f for f in os.listdir('temp/excel/download') if f.endswith('.xlsx')]
total_files = len(files)

print(f"Traitement de {total_files} fichiers Excel...")

for idx, filename in enumerate(files):
    if idx % 10 == 0:
        print(f"Progression : {idx}/{total_files} fichiers...")
    
    filepath = os.path.join('temp/excel/download', filename)
    
    # Extraire le numéro de ward
    try:
        ward_num = filename.split('_')[1]
    except:
        ward_num = 'unknown'
    
    try:
        # Lire le fichier Excel
        df = pd.read_excel(filepath)
        
        # Ajouter des métadonnées
        df['ward'] = ward_num
        df['source_file'] = filename
        df['import_date'] = datetime.now()
        
        # Ajouter aux données
        all_data.append(df)
        
        # Stats
        stats['files_processed'] += 1
        stats['columns_found'].update(df.columns.tolist())
        
        if ward_num not in stats['wards_data']:
            stats['wards_data'][ward_num] = {'files': 0, 'rows': 0}
        
        stats['wards_data'][ward_num]['files'] += 1
        stats['wards_data'][ward_num]['rows'] += len(df)
        stats['total_rows'] += len(df)
        
    except Exception as e:
        print(f"Erreur avec {filename}: {e}")
        stats['files_failed'] += 1

print(f"\nFichiers Excel traités : {stats['files_processed']}")
print(f"Fichiers échoués : {stats['files_failed']}")

# Fusionner toutes les données
if all_data:
    print("\nConcaténation des données...")
    merged_df = pd.concat(all_data, ignore_index=True, sort=False)
    
    print(f"Total des lignes : {len(merged_df)}")
    print(f"Total des colonnes : {len(merged_df.columns)}")
    
    # Sauvegarder le fichier Excel consolidé
    print("\nSauvegarde du fichier Excel consolidé...")
    
    # Excel avec plusieurs feuilles
    output_excel = 'temp/excel/processed/johannesburg_consolidated_all_wards.xlsx'
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        # Feuille principale avec toutes les données
        merged_df.to_excel(writer, sheet_name='All_Data', index=False)
        
        # Résumé par ward
        summary_df = pd.DataFrame([
            {
                'Ward': ward,
                'Files': data['files'],
                'Rows': data['rows']
            }
            for ward, data in sorted(stats['wards_data'].items())
        ])
        summary_df.to_excel(writer, sheet_name='Summary_by_Ward', index=False)
        
        # Informations sur les colonnes
        cols_df = pd.DataFrame({
            'Column': sorted(stats['columns_found']),
            'Type': [str(merged_df[col].dtype) if col in merged_df.columns else 'N/A' 
                    for col in sorted(stats['columns_found'])]
        })
        cols_df.to_excel(writer, sheet_name='Column_Info', index=False)
    
    print(f"✓ Excel consolidé créé : {output_excel}")
    
    # Optionnel : créer aussi des fichiers séparés par ward
    create_by_ward = False  # Mettre à True si vous voulez des fichiers séparés
    
    if create_by_ward:
        print("\nCréation des fichiers Excel par ward...")
        for ward in merged_df['ward'].unique():
            ward_df = merged_df[merged_df['ward'] == ward]
            ward_df.to_excel(f'temp/excel/processed/ward_{ward}_consolidated.xlsx', index=False)
    
    # Statistiques finales
    print("\n=== STATISTIQUES FINALES ===")
    print(f"Wards traités : {len(stats['wards_data'])}")
    print(f"Fichiers Excel fusionnés : {stats['files_processed']}")
    print(f"Total des lignes : {stats['total_rows']}")
    print(f"Colonnes uniques : {len(stats['columns_found'])}")
    
    # Top 5 wards par nombre de lignes
    top_wards = sorted(stats['wards_data'].items(), 
                      key=lambda x: x[1]['rows'], 
                      reverse=True)[:5]
    print("\nTop 5 wards par nombre de lignes :")
    for ward, data in top_wards:
        print(f"  Ward {ward}: {data['rows']} lignes")
    
else:
    print("Aucune donnée n'a pu être fusionnée")
PYTHON_EOF

# ========================================
# 5. UPLOAD DES RÉSULTATS
# ========================================
echo -e "\n${BLUE}5. Upload du fichier Excel consolidé...${NC}"

# Upload le fichier Excel principal
if [ -f "temp/excel/processed/johannesburg_consolidated_all_wards.xlsx" ]; then
    # Avec date pour versioning
    gcloud storage cp "temp/excel/processed/johannesburg_consolidated_all_wards.xlsx" \
        "gs://${BUCKET}/${DEST_PATH}/johannesburg_consolidated_all_wards_$(date +%Y%m%d).xlsx"
    
    # Version sans date pour accès facile
    gcloud storage cp "temp/excel/processed/johannesburg_consolidated_all_wards.xlsx" \
        "gs://${BUCKET}/${DEST_PATH}/johannesburg_consolidated_all_wards.xlsx"
    
    echo -e "${GREEN}✓ Fichier Excel consolidé uploadé${NC}"
fi

# ========================================
# 6. CRÉER UN RAPPORT HTML
# ========================================
echo -e "\n${BLUE}6. Création du rapport de consolidation...${NC}"

python3 << 'PYTHON_EOF'
import pandas as pd
import json

# Charger les données
df = pd.read_excel('temp/excel/processed/consolidated_all_wards.xlsx', sheet_name='All_Data')
summary = pd.read_excel('temp/excel/processed/consolidated_all_wards.xlsx', sheet_name='Summary_by_Ward')

# Créer le rapport HTML
html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Rapport de Consolidation Excel - Johannesburg</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background: #333; color: white; padding: 20px; border-radius: 8px; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .stat-box {{ background: #f0f0f0; padding: 20px; border-radius: 8px; text-align: center; }}
        .number {{ font-size: 36px; font-weight: bold; color: #4CAF50; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #4CAF50; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Rapport de Consolidation Excel</h1>
        <p>Johannesburg - {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}</p>
    </div>
    
    <div class="stats">
        <div class="stat-box">
            <div class="number">{len(summary)}</div>
            <div>Wards traités</div>
        </div>
        <div class="stat-box">
            <div class="number">{len(df):,}</div>
            <div>Lignes totales</div>
        </div>
        <div class="stat-box">
            <div class="number">{len(df.columns)}</div>
            <div>Colonnes</div>
        </div>
    </div>
    
    <h2>Résumé par Ward</h2>
    <table>
        <tr>
            <th>Ward</th>
            <th>Fichiers</th>
            <th>Lignes</th>
        </tr>
        {summary.to_html(index=False, header=False, table_id=None)}
    </table>
    
    <h2>Colonnes disponibles</h2>
    <ul>
        {''.join([f"<li>{col}</li>" for col in sorted(df.columns)])}
    </ul>
</body>
</html>
"""

with open('temp/consolidation_report.html', 'w') as f:
    f.write(html_content)

print("✓ Rapport HTML créé")
PYTHON_EOF

# Upload du rapport
gcloud storage cp "temp/consolidation_report.html" \
    "gs://${BUCKET}/${DEST_PATH}/consolidation_report.html"

# ========================================
# 7. NETTOYAGE
# ========================================
echo -e "\n${BLUE}7. Nettoyage...${NC}"
read -p "Voulez-vous supprimer les fichiers temporaires ? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf temp/
    echo -e "${GREEN}✓ Fichiers temporaires supprimés${NC}"
fi

# ========================================
# RÉSUMÉ FINAL
# ========================================
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}    CONSOLIDATION TERMINÉE !${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""
echo -e "${GREEN}Fichier Excel créé :${NC}"
echo "  • johannesburg_consolidated_all_wards.xlsx"
echo ""
echo -e "${GREEN}Contenu :${NC}"
echo "  • Feuille 'All_Data' : Toutes les données consolidées"
echo "  • Feuille 'Summary_by_Ward' : Résumé par ward"
echo "  • Feuille 'Column_Info' : Information sur les colonnes"
echo ""
echo -e "${GREEN}Emplacement :${NC}"
echo "  gs://${BUCKET}/${DEST_PATH}/"
echo ""
echo -e "${GREEN}✓ Tous les fichiers Excel ont été consolidés en un seul !${NC}"