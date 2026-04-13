# Serve the app on localhost
# Usage: .\serve.ps1   (or: powershell -ExecutionPolicy Bypass -File serve.ps1)

$port = 3000
$url = "http://localhost:$port"

if (Get-Command node -ErrorAction SilentlyContinue) {
    Write-Host "Starting Node server (API-enabled) at $url" -ForegroundColor Green
    Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray
    node server.js
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    Write-Host "Starting Python HTTP server at $url" -ForegroundColor Yellow
    Write-Host "Note: API endpoints under /api/* will NOT work with this server." -ForegroundColor Yellow
    Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray
    python -m http.server $port
} elseif (Get-Command php -ErrorAction SilentlyContinue) {
    Write-Host "Starting PHP server at $url" -ForegroundColor Yellow
    Write-Host "Note: API endpoints under /api/* will NOT work with this server." -ForegroundColor Yellow
    Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray
    php -S "localhost:$port"
} else {
    Write-Host "Node.js, Python, and PHP not found." -ForegroundColor Red
    Write-Host "Options:" -ForegroundColor Yellow
    Write-Host "  1. Install Node.js, then run: node server.js"
    Write-Host "  2. Install Python from https://www.python.org/downloads/ then run this script again (static only)."
    Write-Host "  3. In Cursor: install 'Live Server' extension, then right-click index.html -> Open with Live Server (static only)."
    Write-Host "See SERVE_ALTERNATIVES.md for more options."
    exit 1
}
