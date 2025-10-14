# UIT Document RAG System
**Hệ thống thu thập và xử lý tài liệu UIT cho RAG Chatbot**

---

## 📋 Tổng Quan

Hệ thống tự động crawl và xử lý tài liệu từ website UIT:
1. **Crawler với BFS** - Tự động tìm và tải PDF, DOCX, XLSX từ UIT
2. **OCR Universal** - Xử lý TẤT CẢ định dạng → Text thuần túy
3. **Training Ready** - Dữ liệu sẵn sàng cho RAG chatbot

### 🎯 Kết Quả
- ✅ **429 files** đã thu thập (300 PDF + 112 DOCX + 17 XLSX)
- ✅ **400+ text files** đã xử lý (~15M ký tự)
- ✅ **Tỷ lệ thành công**: ~90%

## 📂 Cấu Trúc Project

```
project_uit_crawl/
├── run_crawl.py              # Script crawler chính
├── crawl_config/             # Cấu hình crawler
│   ├── crawler.yaml          # Settings (max_pages, depth, etc.)
│   └── domains.yaml          # Seeds URLs (47 seeds)
│
├── OCR/                      # Hệ thống OCR ⭐
│   ├── process_all_documents.py   # Xử lý PDF/DOCX/XLSX → Text
│   ├── README_OCR_SYSTEM.md       # Hướng dẫn chi tiết
│   └── data_ocr/                  # DỮ LIỆU ĐÃ XỬ LÝ
│       ├── text/              # Text files (.txt)
│       └── json/              # Metadata JSON
│
├── output_raw/               # Dữ liệu thô từ crawler
│   ├── files/                # 300 PDF + 112 DOCX + 17 XLSX
│   ├── html/                 # HTML pages
│   ├── meta/                 # Metadata
│   ├── overview.json         # Tổng quan dataset
│   ├── overview.md           # Report markdown
│   └── manifest.csv          # Danh sách files
│
├── logs/                     # Logs
│   ├── crawl.log            # Activity log
│   └── seen.json            # Visited URLs
│
├── README.md                # File này
├── README_BFS_CRAWLER.md    # Chi tiết crawler
├── README_OCR_SETUP.md      # Hướng dẫn setup OCR
├── requirements_ocr.txt     # Dependencies
└── setup_ocr.ps1            # Setup script (Windows)
```

---

## 🚀 Quick Start

### 1️⃣ Setup
```powershell
# Cài đặt dependencies
pip install -r requirements_ocr.txt

# Hoặc dùng setup script
.\setup_ocr.ps1
```

### 2️⃣ Crawl Tài Liệu
```powershell
python run_crawl.py
```
- ⏱️ **Thời gian**: 30-60 phút
- 📊 **Kết quả**: 100-500 PDFs + DOCX + XLSX
- 🔄 **BFS Crawler**: Tự động tìm links, khám phá 6 levels deep

### 3️⃣ Xử Lý OCR
```powershell
python OCR/process_all_documents.py
```
- 📝 Nhập `n` = Nhanh (chỉ PDF text-based, 5 phút)
- 📝 Nhập `y` = Chậm (bao gồm OCR scan, 30 phút)
- 💾 **Output**: `OCR/data_ocr/text/*.txt`

### 4️⃣ Sử Dụng Dữ Liệu
```python
from pathlib import Path

# Load corpus
corpus = []
for txt in Path("OCR/data_ocr/text").glob("*.txt"):
    corpus.append(txt.read_text(encoding="utf-8"))

# Sử dụng với LangChain, LLaMA, GPT...
```

---

## ⚙️ Crawler với BFS (Breadth-First Search)

### Tính Năng Nâng Cao
- ✅ **Tự động tìm links**: Không chỉ crawl seeds, còn follow tất cả links
- ✅ **BFS Queue**: Khám phá từng level (depth 0 → 6)
- ✅ **Smart filtering**: Chỉ crawl domains được phép, tránh duplicate
- ✅ **Progress tracking**: Hiển thị real-time: fetched/queue/files/depth

### Cấu Hình (crawl_config/crawler.yaml)
```yaml
max_pages: 10000    # Tối đa 10,000 pages
max_depth: 6        # Độ sâu 6 levels
concurrency: 6      # 6 downloads song song
timeout_ms: 45000   # 45 giây timeout
```

### Seeds (47 URLs trong domains.yaml)
- 📚 Văn bản hành chính
- 🎓 Đào tạo
- 📰 Tin tức/Thông báo
- 🏫 Các khoa (FIT, CE, etc.)
- 🔬 Nghiên cứu

### Kết Quả Dự Kiến
- **Pages**: 500-2,000 visited
- **Files**: 100-500 documents
- **Time**: 30-60 phút
- **Depth**: 4-6 levels

---

## 🔍 OCR Universal Processor

### Hỗ Trợ Formats
| Format | Method | Speed |
|--------|--------|-------|
| **PDF text** | pdfplumber | ⚡ Nhanh |
| **PDF scan** | Tesseract OCR | 🐢 Chậm |
| **DOCX** | python-docx | ⚡ Nhanh |
| **XLSX** | pandas | ⚡ Nhanh |
| **DOC** | Binary parse | ⚠️ Giới hạn |

### Phương Pháp
1. **PDF**: pdfplumber → PyPDF2 → OCR (fallback chain)
2. **DOCX**: Extract paragraphs + tables
3. **XLSX**: Convert all sheets → text tables

### Output
- `OCR/data_ocr/text/` - Text files (UTF-8)
- `OCR/data_ocr/json/` - Metadata (method, char_count, processed_at)

---

## 📊 Thống Kê Project

### Dữ Liệu Thu Thập
- **300 PDFs** (~650 MB)
- **112 DOCX** (~280 MB)
- **17 XLSX** (~42 MB)
- **Tổng: 429 files** (~972 MB)

### Dữ Liệu Đã Xử Lý
- **400+ text files** (~36 MB)
- **~15,000,000 ký tự**
- **~90% thành công**

---

## 🎯 Monitoring & Logs

### Real-time Progress
```
[PROGRESS] Fetched: 50/10000, Queue: 125, Files: 15, Depth: 2
[BFS] Added 12 new links to queue from https://...
[FILE] Downloaded: abc123.pdf (15 total)
```

### Check Status
```powershell
# Đếm PDFs
(Get-ChildItem output_raw/files/*.pdf).Count

# Xem log
Get-Content logs/crawl.log -Tail 20

# Kiểm tra text đã xử lý
(Get-ChildItem OCR/data_ocr/text/*.txt).Count
```

## 🎯 Use Cases

### 1. RAG Chatbot
```python
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma

texts = []
for f in Path("OCR/data_ocr/text").glob("*.txt"):
    texts.append(f.read_text(encoding="utf-8"))

vectorstore = Chroma.from_texts(texts, OpenAIEmbeddings())
```

### 2. Fine-tuning LLM
```python
import json

training_data = []
for json_file in Path("OCR/data_ocr/json").glob("*.json"):
    data = json.loads(json_file.read_text())
    training_data.append({
        "text": data["text"],
        "source": data["filename"]
    })
```

### 3. Search Index
```python
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, ID

schema = Schema(id=ID(stored=True), content=TEXT)
ix = create_in("indexdir", schema)

writer = ix.writer()
for txt in Path("OCR/data_ocr/text").glob("*.txt"):
    writer.add_document(
        id=txt.stem,
        content=txt.read_text(encoding="utf-8")
    )
writer.commit()
```

---

## ⚙️ Requirements

### Bắt Buộc
```bash
pip install PyPDF2 pdfplumber python-docx openpyxl pandas
```

### Optional (PDF Scan OCR)
- **Tesseract OCR**: https://github.com/UB-Mannheim/tesseract/wiki
- **Vietnamese data**: https://github.com/tesseract-ocr/tessdata

---

## � Troubleshooting

| Vấn Đề | Giải Pháp |
|--------|-----------|
| Crawler chậm | Tăng `concurrency` hoặc giảm `max_depth` |
| Không tìm thấy PDFs | Kiểm tra `logs/crawl.log`, tăng `max_depth` |
| OCR lỗi | Chọn `n` (bỏ OCR), chỉ xử lý PDF text-based |
| File không đọc được | Check `OCR/data_ocr/json/` xem method & char_count |

---

## 🔗 Links

- **GitHub**: [UIT_doc_RAG](https://github.com/Jajajou/UIT_doc_RAG) (Branch: `raw_crawl`)
- **Created**: October 2025
- **License**: Educational use only

---

**✅ Project đã sẵn sàng sử dụng!**
