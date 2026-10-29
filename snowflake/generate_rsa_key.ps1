param(
    [string]$KeyDirectory = "$env:USERPROFILE\.snowflake",
    [string]$KeyName = "rsa_key"
)

$opensslCommand = Get-Command openssl -ErrorAction SilentlyContinue
$opensslPath = if ($opensslCommand) {
    $opensslCommand.Source
} else {
    "C:\Program Files\Git\usr\bin\openssl.exe"
}

if (-not (Test-Path $opensslPath)) {
    throw "OpenSSL not found. Install Git for Windows or add openssl to PATH."
}

New-Item -ItemType Directory -Force -Path $KeyDirectory | Out-Null

$privateKeyPath = Join-Path $KeyDirectory "$KeyName.p8"
$publicKeyPath = Join-Path $KeyDirectory "$KeyName.pub"

if (Test-Path $privateKeyPath) {
    throw "Refusing to overwrite existing key: $privateKeyPath"
}

& $opensslPath genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out $privateKeyPath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to generate the private key."
}

& $opensslPath pkey -in $privateKeyPath -pubout -out $publicKeyPath
if ($LASTEXITCODE -ne 0) {
    throw "Failed to generate the public key."
}

$publicKeyBody = (Get-Content $publicKeyPath -Raw) `
    -replace "-----BEGIN PUBLIC KEY-----", "" `
    -replace "-----END PUBLIC KEY-----", "" `
    -replace "\s", ""

Write-Host "Private key: $privateKeyPath"
Write-Host "Public key:  $publicKeyPath"
Write-Host ""
Write-Host "Register the public key in Snowflake as ACCOUNTADMIN:"
Write-Host ""
Write-Host "ALTER USER <your-snowflake-user> SET RSA_PUBLIC_KEY='$publicKeyBody';"
Write-Host ""
Write-Host "Then add this line to .env and remove SNOWFLAKE_PASSWORD:"
Write-Host ""
Write-Host "SNOWFLAKE_PRIVATE_KEY_PATH=$privateKeyPath"
