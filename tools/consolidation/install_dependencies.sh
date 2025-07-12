#!/bin/bash

# ========================================
# INSTALLATION DES DEPENDANCES
# ========================================

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}=== Installation des dépendances ===${NC}"

# Détecter le système
if [[ "$OSTYPE" == "darwin"* ]]; then
    SYSTEM="macOS"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    SYSTEM="Linux"
else
    SYSTEM="Unknown"
fi

echo -e "Système détecté : ${BLUE}$SYSTEM${NC}"

# Vérifier Python
echo -e "\n${BLUE}1. Vérification de Python...${NC}"
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✓ Python installé : $PYTHON_VERSION${NC}"
else
    echo -e "${RED}✗ Python3 non trouvé${NC}"
    if [[ "$SYSTEM" == "macOS" ]]; then
        echo "Installation avec Homebrew..."
        brew install python3
    else
        echo "Installation avec apt..."
        sudo apt-get update
        sudo apt-get install -y python3 python3-pip
    fi
fi

# Vérifier pip
echo -e "\n${BLUE}2. Vérification de pip...${NC}"
if command -v pip3 &> /dev/null; then
    PIP_VERSION=$(pip3 --version)
    echo -e "${GREEN}✓ pip installé : $PIP_VERSION${NC}"
else
    echo -e "${YELLOW}Installation de pip...${NC}"
    if [[ "$SYSTEM" == "macOS" ]]; then
        python3 -m ensurepip --upgrade
    else
        sudo apt-get install -y python3-pip
    fi
fi

# Mettre à jour pip
echo -e "\n${BLUE}3. Mise à jour de pip...${NC}"
pip3 install --upgrade pip

# Installer les packages nécessaires
echo -e "\n${BLUE}4. Installation des packages Python...${NC}"

# Liste des packages
packages=(
    "pandas"
    "openpyxl"  # Pour lire/écrire Excel
    "xlrd"      # Pour lire les anciens fichiers Excel
    "networkx"  # Pour les graphes (optionnel)
    "beautifulsoup4"  # Pour parser HTML (optionnel)
)

for package in "${packages[@]}"; do
    echo -ne "Installation de $package..."
    if pip3 install "$package" &>/dev/null; then
        echo -e " ${GREEN}✓${NC}"
    else
        echo -e " ${RED}✗${NC}"
    fi
done

# Vérifier jq
echo -e "\n${BLUE}5. Vérification de jq...${NC}"
if command -v jq &> /dev/null; then
    JQ_VERSION=$(jq --version)
    echo -e "${GREEN}✓ jq installé : $JQ_VERSION${NC}"
else
    echo -e "${YELLOW}Installation de jq...${NC}"
    if [[ "$SYSTEM" == "macOS" ]]; then
        brew install jq
    else
        sudo apt-get install -y jq
    fi
fi

# Vérifier bc (pour les calculs)
echo -e "\n${BLUE}6. Vérification de bc...${NC}"
if command -v bc &> /dev/null; then
    echo -e "${GREEN}✓ bc installé${NC}"
else
    echo -e "${YELLOW}Installation de bc...${NC}"
    if [[ "$SYSTEM" == "macOS" ]]; then
        brew install bc
    else
        sudo apt-get install -y bc
    fi
fi

# Test final
echo -e "\n${BLUE}7. Test des installations...${NC}"

# Test Python et pandas
python3 << 'EOF'
import sys
print(f"Python : {sys.version}")

try:
    import pandas as pd
    print(f"✓ Pandas : {pd.__version__}")
except ImportError:
    print("✗ Pandas non disponible")

try:
    import openpyxl
    print(f"✓ Openpyxl : {openpyxl.__version__}")
except ImportError:
    print("✗ Openpyxl non disponible")

try:
    import networkx as nx
    print(f"✓ NetworkX : {nx.__version__}")
except ImportError:
    print("✗ NetworkX non disponible")
EOF

echo -e "\n${GREEN}✓ Installation terminée !${NC}"
echo ""
echo "Vous pouvez maintenant exécuter le script de fusion :"
echo "  ./consolidate_all_johannesburg.sh"