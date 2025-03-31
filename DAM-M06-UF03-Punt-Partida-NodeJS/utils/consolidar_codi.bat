@echo off
REM Activa l'entorn Conda especificat
call conda activate mp06-uf03

REM Estableix la pàgina de codis a UTF-8 per a la sortida
chcp 65001 > nul

REM Executa l'script de Python "consolidar_codi.py" amb diferents paràmetres
python consolidar_codi.py --prefix=exemple-mongodb --dir=..\exemple-mongodb

REM Restableix la pàgina de codis a la predeterminada (opcional)
chcp > nul

REM Pausa la finestra perquè puguis veure la sortida abans que es tanqui (opcional)
pause