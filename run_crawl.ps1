# Seeds to crawl (user-provided list + previous targets)
$SEEDS = @(
    'https://www.sig.gov.bf/',
    'https://www.presidencedufaso.bf/',
    'https://www.finances.gov.bf/',
    'https://dgi.bf/',
    'https://www.jobf.gov.bf/',
    'https://servicepublic.gov.bf/',
    'https://www.commerce.gov.bf/',
    'https://www.investburkina.com/',
    'https://www.insd.bf/',
    'https://douanes.bf/',
    'https://www.brvm.org/',
    'https://lefaso.net/',
    'https://burkina24.com/',
    'https://www.sidwaya.info/',
    'https://www.aib.media/',
    'https://esintax.bf/',
    'https://www.cci.bf/',
    'https://www.businessprocedures.bf/',
    'https://www.pndes.gov.bf/',
    'https://www.banquemondiale.org/',
    'https://www.imf.org/',
    'https://www.bceao.int/',
    'https://archive.doingbusiness.org/',
    # Additional seeds provided by user (mix approach)
    'https://www.ministere-sante.gov.bf/',
    'https://www.education.gov.bf/',
    'https://www.ministere-des-ressources-humaines.bf/',
    'https://www.ministere-mine.bf/',
    'https://www.transport.gov.bf/',
    'https://www.environnement.gov.bf/',
    'https://www.agriculture.gov.bf/',
    'https://www.cci.bf/',
    'https://www.onsite-universite-ouagadougou.bf/',
    'https://www.uemoa.int/',
    'https://www.afdb.org/en/countries/west-africa',
    'https://www.orabank.net/',
    'https://www.sfd-bf.org/'
)
# Configurable crawl parameters (mix approach: increase pages per seed)
$MAX_PAGES = 100
$DELAY = 1

# Activate the Python venv
.\\.venv\\Scripts\\Activate.ps1

foreach ($url in $SEEDS) {
    Write-Host "==> Crawling $url (max $MAX_PAGES pages)" -ForegroundColor Green
    python .\scripts\crawl_site.py --start-url $url --out-dir .\data\raw_html --max-pages $MAX_PAGES --delay $DELAY --user-agent "DataCollectorBot/1.0 (respectful)"
    # Short pause between seeds to be polite
    Start-Sleep -Seconds 2
}

# Final report
Write-Host "`nRapport final:" -ForegroundColor Yellow
Get-ChildItem .\data\raw_html\raw -Recurse -File | Measure-Object | ForEach-Object { Write-Host "Total pages téléchargées: $($_.Count)" -ForegroundColor Cyan }