#!/bin/bash

# Activa l'entorn Conda especificat
# En Bash, no podem utilitzar "call" com en Batch
# Depenent de com estigui configurat Conda, podem necessitar una d'aquestes opcions:

# Opció 1: Si Conda està inicialitzat en el shell
conda activate mp06-uf03

# Opció 2: Si necessitem inicialitzar Conda primer (descomenta si és necessari)
# source ~/miniconda3/etc/profile.d/conda.sh || source ~/anaconda3/etc/profile.d/conda.sh
# conda activate mp06-uf03

# Estableix la codificació a UTF-8 per a la sortida (normalment ja és predeterminada en Linux/Mac)
export PYTHONIOENCODING=UTF-8

# Executa l'script de Python "consolidar_codi.py" amb diferents paràmetres
# Nota: Utilitzem "../" en lloc de "..\" per als camins en Linux/Mac
python consolidar_codi.py --prefix=exemple-mongodb --dir=../exemple-mongodb

# Pausa la finestra perquè puguis veure la sortida abans que es tanqui (opcional)
read -p "Prem ENTER per continuar..."
