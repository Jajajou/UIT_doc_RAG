#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Dict, List, Optional
import re
from datetime import datetime

# PDF processing
import PyPDF2
import pdfplumber
from PIL import Image
try:
    import pytesseract
    from pdf2image import convert_from_path
    OCR_LIBS = True
except ImportError:
    OCR_LIBS = False
    print("⚠️ Tesseract/pdf2image không có - OCR bị tắt")

# Word processing
try:
    import docx
    DOCX_LIB = True
except ImportError:
    DOCX_LIB = False
    print("⚠️ python-docx không có - DOCX bị tắt")

# Excel processing
try:
    import openpyxl
    import pandas as pd
    EXCEL_LIBS = True
except ImportError:
    EXCEL_LIBS = False
    print("⚠️ openpyxl/pandas không có - Excel bị tắt")

# Đường dẫn
INPUT_FILES = Path("output_raw/files")
INPUT_META = Path("output_raw/meta")
OCR_DIR = Path("OCR")
DATA_OCR_DIR = OCR_DIR / "data_ocr"
DATA_OCR_TEXT = DATA_OCR_DIR / "text"
DATA_OCR_JSON = DATA_OCR_DIR / "json"

class UniversalDocumentProcessor:
    """Xử lý mọi loại tài liệu"""
    
    def __init__(self, tesseract_path: Optional[str] = None):
        if tesseract_path:
            pytesseract.pytesseract.tesseract_cmd = tesseract_path
        
        # Kiểm tra Tesseract
        try:
            pytesseract.get_tesseract_version()
            self.ocr_available = True
            print("✓ Tesseract OCR có sẵn")
        except:
            print("⚠️ Tesseract không tìm thấy - OCR sẽ bị tắt cho PDF scan")
            self.ocr_available = False
    
    # ==================== PDF PROCESSING ====================
    
    def extract_pdf_text(self, pdf_path: Path, use_ocr: bool = True) -> Dict:
        """Trích xuất text từ PDF"""
        print(f"\n📄 Xử lý PDF: {pdf_path.name}")
        
        result = {
            "filename": pdf_path.name,
            "type": "pdf",
            "text": "",
            "method": None,
            "char_count": 0,
            "processed_at": datetime.now().isoformat(),
        }
        
        # Thử pdfplumber (tốt nhất cho PDF có text)
        try:
            text = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
            
            if text.strip() and len(text.strip()) > 100:
                result["text"] = text.strip()
                result["method"] = "pdfplumber"
                print(f"   ✓ Trích xuất bằng pdfplumber: {len(text)} ký tự")
        except Exception as e:
            print(f"   ⚠ pdfplumber lỗi: {e}")
        
        # Nếu không có text, thử PyPDF2
        if not result["text"]:
            try:
                text = ""
                with open(pdf_path, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    for page in reader.pages:
                        text += page.extract_text() + "\n\n"
                
                if text.strip() and len(text.strip()) > 100:
                    result["text"] = text.strip()
                    result["method"] = "pypdf2"
                    print(f"   ✓ Trích xuất bằng PyPDF2: {len(text)} ký tự")
            except Exception as e:
                print(f"   ⚠ PyPDF2 lỗi: {e}")
        
        # Nếu vẫn không có text và có OCR -> PDF scan
        if not result["text"] and use_ocr and self.ocr_available:
            try:
                print(f"   🔍 Đây là PDF scan - chạy OCR...")
                images = convert_from_path(pdf_path, dpi=300)
                
                text = ""
                for i, image in enumerate(images, 1):
                    print(f"   📄 OCR trang {i}/{len(images)}...")
                    page_text = pytesseract.image_to_string(image, lang='vie+eng')
                    text += f"\n--- Trang {i} ---\n{page_text}\n"
                
                if text.strip():
                    result["text"] = text.strip()
                    result["method"] = "ocr"
                    print(f"   ✓ OCR hoàn thành: {len(text)} ký tự")
            except Exception as e:
                print(f"   ✗ OCR lỗi: {e}")
        
        result["char_count"] = len(result["text"])
        return result
    
    # ==================== WORD PROCESSING ====================
    
    def extract_docx_text(self, docx_path: Path) -> Dict:
        """Trích xuất text từ DOCX"""
        print(f"\n📝 Xử lý DOCX: {docx_path.name}")
        
        result = {
            "filename": docx_path.name,
            "type": "docx",
            "text": "",
            "method": "python-docx",
            "char_count": 0,
            "processed_at": datetime.now().isoformat(),
        }
        
        if not DOCX_LIB:
            print(f"   ✗ python-docx không được cài đặt")
            return result
        
        try:
            doc = docx.Document(docx_path)
            
            # Trích xuất paragraphs
            paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
            text = "\n\n".join(paragraphs)
            
            # Trích xuất tables
            for table in doc.tables:
                for row in table.rows:
                    row_text = " | ".join([cell.text for cell in row.cells])
                    text += "\n" + row_text
            
            result["text"] = text.strip()
            result["char_count"] = len(result["text"])
            print(f"   ✓ Trích xuất: {result['char_count']} ký tự")
            
        except Exception as e:
            print(f"   ✗ Lỗi: {e}")
        
        return result
    
    def extract_doc_text(self, doc_path: Path) -> Dict:
        """Trích xuất text từ DOC (định dạng cũ)"""
        print(f"\n📝 Xử lý DOC: {doc_path.name}")
        
        result = {
            "filename": doc_path.name,
            "type": "doc",
            "text": "",
            "method": "antiword",
            "char_count": 0,
            "processed_at": datetime.now().isoformat(),
        }
        
        # DOC format cũ rất khó xử lý bằng Python
        # Có thể cần antiword hoặc LibreOffice
        print(f"   ⚠️ DOC format cũ - đề xuất chuyển sang DOCX trước")
        
        # Thử đọc binary và tìm text (phương pháp đơn giản)
        try:
            with open(doc_path, 'rb') as f:
                content = f.read()
                # Tìm các chuỗi ASCII/UTF-8
                text = content.decode('latin-1', errors='ignore')
                # Lọc ký tự không mong muốn
                text = re.sub(r'[^\x20-\x7E\u00C0-\u1EF9\n]', '', text)
                # Lọc dòng ngắn
                lines = [l for l in text.split('\n') if len(l.strip()) > 5]
                text = '\n'.join(lines)
                
                if text.strip():
                    result["text"] = text.strip()
                    result["char_count"] = len(result["text"])
                    print(f"   ⚠️ Trích xuất binary: {result['char_count']} ký tự (có thể không chính xác)")
        except Exception as e:
            print(f"   ✗ Lỗi: {e}")
        
        return result
    
    # ==================== EXCEL PROCESSING ====================
    
    def extract_xlsx_text(self, xlsx_path: Path) -> Dict:
        """Trích xuất text từ XLSX"""
        print(f"\n📊 Xử lý XLSX: {xlsx_path.name}")
        
        result = {
            "filename": xlsx_path.name,
            "type": "xlsx",
            "text": "",
            "method": "openpyxl",
            "char_count": 0,
            "processed_at": datetime.now().isoformat(),
        }
        
        if not EXCEL_LIBS:
            print(f"   ✗ pandas/openpyxl không được cài đặt")
            return result
        
        try:
            # Đọc tất cả các sheet
            excel_file = pd.ExcelFile(xlsx_path)
            all_text = []
            
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
                
                # Thêm tên sheet
                all_text.append(f"\n{'='*60}")
                all_text.append(f"SHEET: {sheet_name}")
                all_text.append('='*60 + '\n')
                
                # Chuyển DataFrame thành text
                # Header
                all_text.append(" | ".join(str(col) for col in df.columns))
                all_text.append("-" * 80)
                
                # Data rows
                for idx, row in df.iterrows():
                    row_text = " | ".join(str(val) for val in row.values if pd.notna(val))
                    if row_text.strip():
                        all_text.append(row_text)
            
            result["text"] = "\n".join(all_text)
            result["char_count"] = len(result["text"])
            print(f"   ✓ Trích xuất {len(excel_file.sheet_names)} sheets: {result['char_count']} ký tự")
            
        except Exception as e:
            print(f"   ✗ Lỗi: {e}")
        
        return result
    
    def extract_xls_text(self, xls_path: Path) -> Dict:
        """Trích xuất text từ XLS"""
        print(f"\n📊 Xử lý XLS: {xls_path.name}")
        
        # Pandas có thể đọc cả XLS và XLSX
        result = {
            "filename": xls_path.name,
            "type": "xls",
            "text": "",
            "method": "pandas",
            "char_count": 0,
            "processed_at": datetime.now().isoformat(),
        }
        
        if not EXCEL_LIBS:
            print(f"   ✗ pandas/openpyxl không được cài đặt")
            return result
        
        try:
            excel_file = pd.ExcelFile(xls_path)
            all_text = []
            
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(xls_path, sheet_name=sheet_name)
                
                all_text.append(f"\n{'='*60}")
                all_text.append(f"SHEET: {sheet_name}")
                all_text.append('='*60 + '\n')
                
                all_text.append(" | ".join(str(col) for col in df.columns))
                all_text.append("-" * 80)
                
                for idx, row in df.iterrows():
                    row_text = " | ".join(str(val) for val in row.values if pd.notna(val))
                    if row_text.strip():
                        all_text.append(row_text)
            
            result["text"] = "\n".join(all_text)
            result["char_count"] = len(result["text"])
            print(f"   ✓ Trích xuất {len(excel_file.sheet_names)} sheets: {result['char_count']} ký tự")
            
        except Exception as e:
            print(f"   ✗ Lỗi: {e}")
        
        return result
    
    # ==================== UNIVERSAL PROCESSOR ====================
    
    def process_file(self, file_path: Path, use_ocr: bool = True) -> Optional[Dict]:
        """Xử lý bất kỳ loại file nào"""
        ext = file_path.suffix.lower()
        
        if ext == '.pdf':
            return self.extract_pdf_text(file_path, use_ocr)
        elif ext == '.docx':
            return self.extract_docx_text(file_path)
        elif ext == '.doc':
            return self.extract_doc_text(file_path)
        elif ext in ['.xlsx', '.xlsm']:
            return self.extract_xlsx_text(file_path)
        elif ext == '.xls':
            return self.extract_xls_text(file_path)
        else:
            print(f"\n⚠️ Bỏ qua: {file_path.name} (định dạng không hỗ trợ: {ext})")
            return None


def main():
    """Main execution"""
    print("=" * 80)
    print("🔍 UNIVERSAL DOCUMENT OCR PROCESSOR")
    print("Xử lý: PDF, DOCX, DOC, XLSX, XLS -> Text")
    print("=" * 80)
    
    # Tạo thư mục
    for path in [OCR_DIR, DATA_OCR_DIR, DATA_OCR_TEXT, DATA_OCR_JSON]:
        path.mkdir(parents=True, exist_ok=True)
    
    # Khởi tạo processor
    print("\n[1/5] Khởi tạo processor...")
    processor = UniversalDocumentProcessor()
    
    # Tìm tất cả files
    print("\n[2/5] Tìm tất cả files...")
    if not INPUT_FILES.exists():
        print(f"❌ Thư mục không tồn tại: {INPUT_FILES}")
        print("Chạy crawler trước: python run_crawl.py")
        return
    
    all_files = []
    for ext in ['.pdf', '.docx', '.doc', '.xlsx', '.xls', '.xlsm']:
        all_files.extend(INPUT_FILES.glob(f"*{ext}"))
    
    print(f"   Tìm thấy {len(all_files)} files:")
    file_types = {}
    for f in all_files:
        ext = f.suffix.lower()
        file_types[ext] = file_types.get(ext, 0) + 1
    
    for ext, count in sorted(file_types.items()):
        print(f"   • {ext}: {count} files")
    
    if not all_files:
        print("\n⚠️ Không tìm thấy file nào!")
        return
    
    # Hỏi về OCR
    print("\n[3/5] Cấu hình OCR...")
    print("   OCR cho PDF scan? (chậm nhưng chính xác hơn)")
    use_ocr = input("   Bật OCR? (y/n, mặc định=n): ").lower() == 'y'
    
    # Xử lý files
    print(f"\n[4/5] Xử lý {len(all_files)} files...")
    processed_docs = []
    
    for i, file_path in enumerate(all_files, 1):
        print(f"\n[{i}/{len(all_files)}]", end=" ")
        result = processor.process_file(file_path, use_ocr=use_ocr)
        
        if result and result["text"]:
            # Lưu text
            text_path = DATA_OCR_TEXT / f"{file_path.stem}.txt"
            text_path.write_text(result["text"], encoding="utf-8")
            
            # Lưu JSON metadata
            json_path = DATA_OCR_JSON / f"{file_path.stem}.json"
            json_path.write_text(
                json.dumps(result, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            
            processed_docs.append(result)
            print(f"   ✓ Lưu: {result['char_count']:,} ký tự")
        else:
            print(f"   ✗ Không trích xuất được text")
    
    # Tổng kết
    print("\n[5/5] Tổng kết")
    print("=" * 80)
    print(f"📊 Thống kê:")
    print(f"   • Tổng files: {len(all_files)}")
    print(f"   • Xử lý thành công: {len(processed_docs)}")
    print(f"   • Thất bại: {len(all_files) - len(processed_docs)}")
    print(f"   • Tổng ký tự: {sum(d['char_count'] for d in processed_docs):,}")
    
    # Thống kê theo loại
    by_type = {}
    for doc in processed_docs:
        doc_type = doc['type']
        by_type[doc_type] = by_type.get(doc_type, 0) + 1
    
    print(f"\n📁 Theo loại file:")
    for doc_type, count in sorted(by_type.items()):
        print(f"   • {doc_type.upper()}: {count} files")
    
    print(f"\n📂 Kết quả:")
    print(f"   • Text files: {DATA_OCR_TEXT}/")
    print(f"   • JSON metadata: {DATA_OCR_JSON}/")
    
    print(f"\n💡 Bước tiếp theo:")
    print(f"   1. Kiểm tra text: OCR/data_ocr/text/")
    print(f"   2. Sử dụng cho chatbot training")
    print(f"   3. Tích hợp với RAG system")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
