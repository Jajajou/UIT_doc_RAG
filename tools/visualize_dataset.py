import json
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
import html

ROOT = Path("output_raw")
META = ROOT / "meta"
FILES = ROOT / "files"
HTML_DIR = ROOT / "html"
OUTPUT = Path("tools/dataset_report.html")


def load_metadata():
    """Load all metadata files"""
    items = []
    for jpath in META.glob("*.json"):
        try:
            data = json.loads(jpath.read_text(encoding="utf-8"))
            items.append(data)
        except Exception as e:
            print(f"[ERROR] Failed to load {jpath}: {e}")
    return items


def analyze_data(items):
    """Analyze dataset and compute statistics"""
    stats = {
        "total": len(items),
        "by_status": Counter(),
        "by_content_type": Counter(),
        "by_domain": Counter(),
        "by_extension": Counter(),
        "files_count": 0,
        "html_count": 0,
        "total_links": 0,
        "missing_files": [],
        "urls": [],
        "timeline": defaultdict(int),
    }

    for item in items:
        url = item.get("url", "")
        status = item.get("status")
        ctype = item.get("content_type", "unknown")
        domain = item.get("domain", "unknown")
        fpath = item.get("file_path")
        hpath = item.get("html_path")
        links = item.get("out_links", [])
        fetched = item.get("fetched_at", "")

        # Count by status
        stats["by_status"][status or "None"] += 1

        # Count by content type
        ct_short = ctype.split(";")[0] if ctype else "unknown"
        stats["by_content_type"][ct_short] += 1

        # Count by domain
        stats["by_domain"][domain] += 1

        # Count files vs HTML
        if fpath:
            stats["files_count"] += 1
            ext = Path(fpath).suffix.lower() or ".bin"
            stats["by_extension"][ext] += 1
            # Check if file exists
            if not Path(fpath).exists():
                stats["missing_files"].append(fpath)
        
        if hpath:
            stats["html_count"] += 1
            if not Path(hpath).exists():
                stats["missing_files"].append(hpath)

        # Count links
        stats["total_links"] += len(links)

        # Store URL info
        stats["urls"].append({
            "url": url or "",
            "status": status,
            "type": "file" if fpath else "html",
            "domain": domain or "unknown",
            "links": len(links),
        })

        # Timeline (by date)
        if fetched:
            try:
                date = fetched.split("T")[0]  # Get YYYY-MM-DD
                stats["timeline"][date] += 1
            except:
                pass

    return stats


def format_size(path):
    """Get human-readable file size"""
    if not path.exists():
        return "Missing"
    size = path.stat().st_size
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def calculate_storage_stats():
    """Calculate storage statistics"""
    total_files = 0
    total_html = 0
    total_meta = 0

    for f in FILES.glob("*"):
        if f.is_file():
            total_files += f.stat().st_size

    for f in HTML_DIR.glob("*.html"):
        total_html += f.stat().st_size

    for f in META.glob("*.json"):
        total_meta += f.stat().st_size

    return {
        "files": total_files,
        "html": total_html,
        "meta": total_meta,
        "total": total_files + total_html + total_meta,
    }


def generate_html_report(stats, storage):
    """Generate interactive HTML dashboard"""
    
    # Prepare chart data
    status_labels = list(stats["by_status"].keys())
    status_values = list(stats["by_status"].values())
    
    ctype_labels = list(stats["by_content_type"].keys())
    ctype_values = list(stats["by_content_type"].values())
    
    domain_labels = list(stats["by_domain"].keys())
    domain_values = list(stats["by_domain"].values())
    
    ext_labels = list(stats["by_extension"].keys())
    ext_values = list(stats["by_extension"].values())
    
    timeline_dates = sorted(stats["timeline"].keys())
    timeline_counts = [stats["timeline"][d] for d in timeline_dates]

    # Format storage sizes
    def fmt_size(bytes_val):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_val < 1024:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024
        return f"{bytes_val:.1f} TB"

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UIT Crawler Dataset Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            margin-bottom: 30px;
            text-align: center;
        }}
        .header h1 {{
            color: #667eea;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        .header p {{
            color: #666;
            font-size: 1.1em;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            transition: transform 0.3s, box-shadow 0.3s;
        }}
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0,0,0,0.15);
        }}
        .stat-card h3 {{
            color: #667eea;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }}
        .stat-card .value {{
            font-size: 2.5em;
            font-weight: bold;
            color: #333;
        }}
        .stat-card .label {{
            color: #999;
            font-size: 0.9em;
            margin-top: 5px;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .chart-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
        }}
        .chart-card h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.3em;
        }}
        .chart-container {{
            position: relative;
            height: 300px;
        }}
        .url-list {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .url-list h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.3em;
        }}
        .url-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }}
        .url-table th {{
            background: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        .url-table td {{
            padding: 10px 12px;
            border-bottom: 1px solid #eee;
        }}
        .url-table tr:hover {{
            background: #f8f9ff;
        }}
        .status-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        .status-200 {{ background: #d4edda; color: #155724; }}
        .status-other {{ background: #fff3cd; color: #856404; }}
        .status-none {{ background: #f8d7da; color: #721c24; }}
        .type-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
            background: #e7f3ff;
            color: #004085;
        }}
        .footer {{
            background: white;
            padding: 20px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            text-align: center;
            color: #666;
            margin-top: 30px;
        }}
        .alert {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 20px 0;
            border-radius: 5px;
        }}
        .alert h3 {{
            color: #856404;
            margin-bottom: 10px;
        }}
        .alert ul {{
            margin-left: 20px;
            color: #856404;
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <h1>🎓 UIT Crawler Dataset Report</h1>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <!-- Statistics Cards -->
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Items</h3>
                <div class="value">{stats['total']}</div>
                <div class="label">URLs Crawled</div>
            </div>
            <div class="stat-card">
                <h3>HTML Pages</h3>
                <div class="value">{stats['html_count']}</div>
                <div class="label">Saved Pages</div>
            </div>
            <div class="stat-card">
                <h3>Files</h3>
                <div class="value">{stats['files_count']}</div>
                <div class="label">Downloaded Files</div>
            </div>
            <div class="stat-card">
                <h3>Total Links</h3>
                <div class="value">{stats['total_links']}</div>
                <div class="label">Extracted Links</div>
            </div>
            <div class="stat-card">
                <h3>Total Storage</h3>
                <div class="value">{fmt_size(storage['total'])}</div>
                <div class="label">
                    Files: {fmt_size(storage['files'])}<br>
                    HTML: {fmt_size(storage['html'])}<br>
                    Meta: {fmt_size(storage['meta'])}
                </div>
            </div>
            <div class="stat-card">
                <h3>Domains</h3>
                <div class="value">{len(stats['by_domain'])}</div>
                <div class="label">Unique Domains</div>
            </div>
        </div>

        {f'''<div class="alert">
            <h3>⚠️ Missing Files: {len(stats['missing_files'])}</h3>
            <ul>
                {''.join(f"<li>{html.escape(f)}</li>" for f in stats['missing_files'][:10])}
                {f"<li>... and {len(stats['missing_files']) - 10} more</li>" if len(stats['missing_files']) > 10 else ""}
            </ul>
        </div>''' if stats['missing_files'] else ''}

        <!-- Charts -->
        <div class="chart-grid">
            <div class="chart-card">
                <h2>📊 Status Codes</h2>
                <div class="chart-container">
                    <canvas id="statusChart"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h2>📁 Content Types</h2>
                <div class="chart-container">
                    <canvas id="ctypeChart"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h2>🌐 By Domain</h2>
                <div class="chart-container">
                    <canvas id="domainChart"></canvas>
                </div>
            </div>
            <div class="chart-card">
                <h2>📄 File Extensions</h2>
                <div class="chart-container">
                    <canvas id="extChart"></canvas>
                </div>
            </div>
        </div>

        {f'''<div class="chart-grid">
            <div class="chart-card" style="grid-column: 1 / -1;">
                <h2>📅 Crawl Timeline</h2>
                <div class="chart-container">
                    <canvas id="timelineChart"></canvas>
                </div>
            </div>
        </div>''' if timeline_dates else ''}

        <!-- URL List -->
        <div class="url-list">
            <h2>🔗 Crawled URLs ({len(stats['urls'])})</h2>
            <div style="overflow-x: auto;">
                <table class="url-table">
                    <thead>
                        <tr>
                            <th>URL</th>
                            <th>Status</th>
                            <th>Type</th>
                            <th>Domain</th>
                            <th>Links</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(f'''
                        <tr>
                            <td style="max-width: 500px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="{html.escape(u['url'])}">{html.escape(u['url'])}</td>
                            <td><span class="status-badge status-{200 if u['status'] == 200 else 'other' if u['status'] else 'none'}">{u['status'] or 'N/A'}</span></td>
                            <td><span class="type-badge">{u['type']}</span></td>
                            <td>{html.escape(u['domain'])}</td>
                            <td>{u['links']}</td>
                        </tr>
                        ''' for u in stats['urls'][:100])}
                        {f'<tr><td colspan="5" style="text-align: center; color: #999;">... and {len(stats["urls"]) - 100} more URLs</td></tr>' if len(stats['urls']) > 100 else ''}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Footer -->
        <div class="footer">
            <p>🚀 Generated by UIT Crawler Visualization Tool</p>
            <p style="margin-top: 10px; font-size: 0.9em;">Data source: output_raw/</p>
        </div>
    </div>

    <script>
        const chartConfig = {{
            plugins: {{
                legend: {{
                    position: 'bottom',
                    labels: {{
                        padding: 15,
                        font: {{ size: 12 }}
                    }}
                }}
            }},
            responsive: true,
            maintainAspectRatio: false
        }};

        const colors = [
            '#667eea', '#764ba2', '#f093fb', '#4facfe',
            '#43e97b', '#fa709a', '#fee140', '#30cfd0'
        ];

        // Status Chart
        new Chart(document.getElementById('statusChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(status_labels)},
                datasets: [{{
                    data: {json.dumps(status_values)},
                    backgroundColor: colors,
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: chartConfig
        }});

        // Content Type Chart
        new Chart(document.getElementById('ctypeChart'), {{
            type: 'pie',
            data: {{
                labels: {json.dumps(ctype_labels)},
                datasets: [{{
                    data: {json.dumps(ctype_values)},
                    backgroundColor: colors,
                    borderWidth: 2,
                    borderColor: '#fff'
                }}]
            }},
            options: chartConfig
        }});

        // Domain Chart
        new Chart(document.getElementById('domainChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(domain_labels)},
                datasets: [{{
                    label: 'Items per Domain',
                    data: {json.dumps(domain_values)},
                    backgroundColor: '#667eea',
                    borderRadius: 8
                }}]
            }},
            options: {{
                ...chartConfig,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});

        // Extension Chart
        new Chart(document.getElementById('extChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(ext_labels)},
                datasets: [{{
                    label: 'Files by Extension',
                    data: {json.dumps(ext_values)},
                    backgroundColor: '#764ba2',
                    borderRadius: 8
                }}]
            }},
            options: {{
                ...chartConfig,
                plugins: {{
                    legend: {{ display: false }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});

        {f'''// Timeline Chart
        new Chart(document.getElementById('timelineChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(timeline_dates)},
                datasets: [{{
                    label: 'Items Crawled',
                    data: {json.dumps(timeline_counts)},
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderColor: '#667eea',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    pointBackgroundColor: '#667eea'
                }}]
            }},
            options: {{
                ...chartConfig,
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});''' if timeline_dates else ''}
    </script>
</body>
</html>"""
    
    return html_content


def main():
    """Main execution"""
    print("=" * 60)
    print("UIT Crawler Dataset Visualization Tool")
    print("=" * 60)
    
    # Load data
    print("\n[1/4] Loading metadata...")
    items = load_metadata()
    print(f"      ✓ Loaded {len(items)} items")
    
    if not items:
        print("\n[ERROR] No metadata found in output_raw/meta/")
        return
    
    # Analyze
    print("\n[2/4] Analyzing data...")
    stats = analyze_data(items)
    print(f"      ✓ Total items: {stats['total']}")
    print(f"      ✓ HTML pages: {stats['html_count']}")
    print(f"      ✓ Files: {stats['files_count']}")
    print(f"      ✓ Total links: {stats['total_links']}")
    
    # Calculate storage
    print("\n[3/4] Calculating storage...")
    storage = calculate_storage_stats()
    
    # Generate report
    print("\n[4/4] Generating HTML report...")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    html_report = generate_html_report(stats, storage)
    OUTPUT.write_text(html_report, encoding="utf-8")
    print(f"      ✓ Report saved to: {OUTPUT}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total Items:      {stats['total']}")
    print(f"HTML Pages:       {stats['html_count']}")
    print(f"Downloaded Files: {stats['files_count']}")
    print(f"Total Links:      {stats['total_links']}")
    print(f"Unique Domains:   {len(stats['by_domain'])}")
    domains_str = ', '.join(str(d) for d in stats['by_domain'].keys() if d)
    print(f"\nDomains: {domains_str}")
    print(f"\nMissing Files:    {len(stats['missing_files'])}")
    if stats['missing_files']:
        print("  (Check the HTML report for details)")
    
    print("\n" + "=" * 60)
    print(f"✅ Open the report: {OUTPUT.absolute()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
