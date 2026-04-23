#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Visa alla Purview glossary termer med PowerShell
#>

Write-Host "`n" "="*80 -ForegroundColor Cyan
Write-Host "  PURVIEW GLOSSARY TERMER - PowerShell" -ForegroundColor Cyan
Write-Host "="*80 -ForegroundColor Cyan
Write-Host ""

# Hämta token
Write-Host "🔐 Hämtar Azure AD token..." -ForegroundColor Yellow
$token = az account get-access-token --resource https://purview.azure.net --query accessToken -o tsv

if (-not $token) {
    Write-Host "❌ Kunde inte hämta token" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Token hämtad: $($token.Substring(0,20))..." -ForegroundColor Green
Write-Host ""

$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

$baseUrl = "https://prviewacc.purview.azure.com/catalog/api/atlas/v2"

# Hämta glossary
Write-Host "📚 Hämtar glossary..." -ForegroundColor Yellow

try {
    $glossaryResponse = Invoke-RestMethod -Uri "$baseUrl/glossary" -Headers $headers -Method Get
    
    if ($glossaryResponse -is [array]) {
        $glossary = $glossaryResponse[0]
    } else {
        $glossary = $glossaryResponse
    }
    
    Write-Host "✅ Glossary: $($glossary.name)" -ForegroundColor Green
    Write-Host "   GUID: $($glossary.guid)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "   Portal UI: https://purview.microsoft.com/glossary/$($glossary.guid)" -ForegroundColor Cyan
    Write-Host ""
    
} catch {
    Write-Host "❌ Fel vid hämtning av glossary: $_" -ForegroundColor Red
    exit 1
}

# Hämta termer
Write-Host "📊 Hämtar alla termer..." -ForegroundColor Yellow

try {
    $terms = Invoke-RestMethod -Uri "$baseUrl/glossary/$($glossary.guid)/terms?limit=500" -Headers $headers -Method Get
    
    Write-Host "✅ Hittade $($terms.Count) termer" -ForegroundColor Green
    Write-Host ""
    
} catch {
    Write-Host "❌ Fel vid hämtning av termer: $_" -ForegroundColor Red
    exit 1
}

# Gruppera per kategori
$grouped = @{}
$noCategory = @()

foreach ($term in $terms) {
    if ($term.categories -and $term.categories.Count -gt 0) {
        $catName = $term.categories[0].displayText
        if ($catName) {
            if (-not $grouped.ContainsKey($catName)) {
                $grouped[$catName] = @()
            }
            $grouped[$catName] += $term
        } else {
            $noCategory += $term
        }
    } else {
        $noCategory += $term
    }
}

# Visa per kategori
Write-Host "="*80 -ForegroundColor Cyan
Write-Host "  TERMER PER KATEGORI" -ForegroundColor Cyan
Write-Host "="*80 -ForegroundColor Cyan
Write-Host ""

# Termer utan kategori
if ($noCategory.Count -gt 0) {
    Write-Host "📁 Ingen kategori ($($noCategory.Count) termer)" -ForegroundColor Yellow
    Write-Host "-"*80 -ForegroundColor Gray
    $noCategory | Sort-Object name | Select-Object -First 10 | ForEach-Object {
        Write-Host "   • $($_.name)" -ForegroundColor White
        if ($_.shortDescription) {
            $desc = $_.shortDescription
            if ($desc.Length -gt 70) { $desc = $desc.Substring(0, 70) + "..." }
            Write-Host "     $desc" -ForegroundColor DarkGray
        }
    }
    if ($noCategory.Count -gt 10) {
        Write-Host "   ... och $($noCategory.Count - 10) till" -ForegroundColor DarkGray
    }
    Write-Host ""
}

# Kategoriserade termer
foreach ($catName in ($grouped.Keys | Sort-Object)) {
    $catTerms = $grouped[$catName]
    Write-Host "📁 $catName ($($catTerms.Count) termer)" -ForegroundColor Yellow
    Write-Host "-"*80 -ForegroundColor Gray
    $catTerms | Sort-Object name | Select-Object -First 10 | ForEach-Object {
        Write-Host "   • $($_.name)" -ForegroundColor White
        if ($_.shortDescription) {
            $desc = $_.shortDescription
            if ($desc.Length -gt 70) { $desc = $desc.Substring(0, 70) + "..." }
            Write-Host "     $desc" -ForegroundColor DarkGray
        }
    }
    if ($catTerms.Count -gt 10) {
        Write-Host "   ... och $($catTerms.Count - 10) till" -ForegroundColor DarkGray
    }
    Write-Host ""
}

# Statistik
Write-Host "="*80 -ForegroundColor Cyan
Write-Host "  STATISTIK" -ForegroundColor Cyan
Write-Host "="*80 -ForegroundColor Cyan
Write-Host ""

$withDesc = ($terms | Where-Object { $_.shortDescription -or $_.longDescription }).Count
$withCat = ($terms | Where-Object { $_.categories -and $_.categories.Count -gt 0 }).Count

Write-Host "Totalt antal termer: " -NoNewline
Write-Host "$($terms.Count)" -ForegroundColor Green
Write-Host "Termer med beskrivning: " -NoNewline
Write-Host "$withDesc ($([Math]::Round($withDesc * 100 / $terms.Count))%)" -ForegroundColor Green
Write-Host "Termer med kategori: " -NoNewline
Write-Host "$withCat ($([Math]::Round($withCat * 100 / $terms.Count))%)" -ForegroundColor Green
Write-Host "Antal kategorier: " -NoNewline
Write-Host "$($grouped.Keys.Count)" -ForegroundColor Green

Write-Host ""
Write-Host "="*80 -ForegroundColor Cyan
Write-Host "  HUR MAN SER TERMERNA" -ForegroundColor Cyan
Write-Host "="*80 -ForegroundColor Cyan
Write-Host ""
Write-Host "1. GÅ TILL PORTALEN:" -ForegroundColor Yellow
Write-Host "   https://purview.microsoft.com" -ForegroundColor Cyan
Write-Host ""
Write-Host "2. KLICKA PÅ 'GLOSSARY' i vänstermenyn" -ForegroundColor Yellow
Write-Host ""
Write-Host "3. DU SER NU ALLA $($terms.Count) TERMER" -ForegroundColor Yellow
Write-Host ""
Write-Host "4. DIREKT LÄNK:" -ForegroundColor Yellow
Write-Host "   https://purview.microsoft.com/glossary/$($glossary.guid)" -ForegroundColor Cyan
Write-Host ""
Write-Host "="*80 -ForegroundColor Cyan
Write-Host ""
