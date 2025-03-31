# Activa l'entorn Conda especificat
conda activate mp06-uf03

# Estableix la codificació a UTF-8 per a la sortida
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Executa l'script de Python "consolidar_codi.py" amb diferents paràmetres
python consolidar_codi.py --prefix=exemple-mongodb --dir=..\exemple-mongodb

# Restableix la codificació a la predeterminada (opcional)
# En PowerShell normalment no cal restablir-ho explícitament

# Pausa la finestra perquè puguis veure la sortida abans que es tanqui (opcional)
Write-Host "Prem una tecla per continuar..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
