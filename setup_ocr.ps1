# Quick Setup Script for UIT PDF Crawler with OCR
# Run this in PowerShell

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "UIT PDF Crawler + OCR Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check Python
Write-Host "[1/5] Checking Python..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  ✓ Found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Python not found! Please install Python 3.8+" -ForegroundColor Red
    exit 1
}

# Install crawler dependencies
Write-Host ""
Write-Host "[2/5] Installing crawler dependencies..." -ForegroundColor Yellow
python -m pip install --upgrade pip
pip install -r requirements.txt
Write-Host "  ✓ Crawler dependencies installed" -ForegroundColor Green

# Install OCR dependencies
Write-Host ""
Write-Host "[3/5] Installing OCR dependencies..." -ForegroundColor Yellow
pip install -r requirements_ocr.txt
Write-Host "  ✓ OCR dependencies installed" -ForegroundColor Green

# Check Tesseract
Write-Host ""
Write-Host "[4/5] Checking Tesseract OCR..." -ForegroundColor Yellow
$tesseractPath = "C:\Program Files\Tesseract-OCR\tesseract.exe"
if (Test-Path $tesseractPath) {
    $tesseractVersion = & $tesseractPath --version 2>&1 | Select-Object -First 1
    Write-Host "  ✓ Found: $tesseractVersion" -ForegroundColor Green
    
    # Check Vietnamese language
    $vieLang = "C:\Program Files\Tesseract-OCR\tessdata\vie.traineddata"
    if (Test-Path $vieLang) {
        Write-Host "  ✓ Vietnamese language data installed" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Vietnamese language data not found" -ForegroundColor Yellow
        Write-Host "    Download from: https://github.com/tesseract-ocr/tessdata/blob/main/vie.traineddata" -ForegroundColor Yellow
        Write-Host "    Save to: $vieLang" -ForegroundColor Yellow
    }
} else {
    Write-Host "  ⚠ Tesseract OCR not found" -ForegroundColor Yellow
    Write-Host "    Download from: https://github.com/UB-Mannheim/tesseract/wiki" -ForegroundColor Yellow
    Write-Host "    Install to: C:\Program Files\Tesseract-OCR" -ForegroundColor Yellow
    Write-Host "    Note: OCR features will be disabled without Tesseract" -ForegroundColor Yellow
}

# Check Poppler
Write-Host ""
Write-Host "[5/5] Checking Poppler (for PDF to image)..." -ForegroundColor Yellow
try {
    $pdfToPpmTest = Get-Command pdftoppm -ErrorAction Stop
    Write-Host "  ✓ Poppler found in PATH" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ Poppler not found in PATH" -ForegroundColor Yellow
    Write-Host "    Download from: https://github.com/oschwartz10612/poppler-windows/releases/" -ForegroundColor Yellow
    Write-Host "    Extract and add bin\ folder to PATH" -ForegroundColor Yellow
    Write-Host "    Note: OCR for scanned PDFs will not work without Poppler" -ForegroundColor Yellow
}

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Run crawler:        python run_crawl.py" -ForegroundColor White
Write-Host "2. Process PDFs:       python tools/process_pdfs_with_ocr.py" -ForegroundColor White
Write-Host "3. View results:       output_processed/training_data/" -ForegroundColor White
Write-Host ""
Write-Host "For detailed instructions, see: README_OCR_SETUP.md" -ForegroundColor Cyan
Write-Host ""
