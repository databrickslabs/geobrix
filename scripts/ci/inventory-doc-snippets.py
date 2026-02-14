#!/usr/bin/env python3
"""
Inventory all code snippets in GeoBrix documentation.

Categorizes snippets as:
1. SELF_CONTAINED - Can be copy-pasted and run immediately
2. EXTENDS_SETUP - Requires common setup section (imports, data)
3. EXAMPLE_ONLY - Illustrative snippet, not meant to be run
4. SHELL_COMMAND - Shell/bash command
5. NEEDS_REVIEW - Unclear category, needs manual review

Output: JSON inventory with snippet details and categorization
"""

import re
import json
from pathlib import Path
from typing import List, Dict
import sys

DOCS_DIR = Path("docs/docs")


def extract_code_blocks(markdown_file: Path) -> List[Dict]:
    """Extract all code blocks from a markdown file"""
    content = markdown_file.read_text()
    
    # Pattern to match code blocks
    pattern = r'```(\w+)\s*\n(.*?)```'
    
    blocks = []
    for match in re.finditer(pattern, content, re.DOTALL):
        lang = match.group(1)
        code = match.group(2)
        line_num = content[:match.start()].count('\n') + 1
        
        # Only process code blocks (not text or output)
        if lang in ['python', 'scala', 'sql', 'bash', 'sh']:
            blocks.append({
                "language": lang,
                "code": code,
                "line_number": line_num,
                "length_lines": code.count('\n') + 1,
                "source_file": str(markdown_file.relative_to("docs/docs")),
            })
    
    return blocks


def analyze_snippet(snippet: Dict) -> Dict:
    """Analyze a code snippet to determine category"""
    code = snippet["code"]
    lang = snippet["language"]
    
    # Indicators of self-contained snippets
    has_imports = "import " in code or "from " in code
    has_spark_creation = "SparkSession.builder" in code or "getOrCreate()" in code
    has_data_creation = "spark.range(" in code or "spark.createDataFrame" in code or "Seq(" in code and ".toDF(" in code
    has_file_reading = "spark.read.format" in code or "spark.read" in code and "load(" in code
    has_rx_registration = "rx.register(spark)" in code or "functions.register(spark)" in code
    
    # Indicators of setup extension
    uses_existing_df = re.search(r'\bdf\b', code) and not ("= spark" in code or "df = " in code)
    uses_existing_rasters = re.search(r'\brasters\b', code) and not ("= spark.read" in code or "rasters = " in code)
    uses_rx_without_import = ("rx." in code or "bx." in code or "vx." in code) and not has_imports
    uses_spark_without_creation = "spark." in code and not has_spark_creation and not "def " in code
    
    # Indicators of shell commands
    is_shell_command = lang in ['bash', 'sh']
    
    # Indicators of example only
    is_short = len(code.strip()) < 50
    has_placeholders = "<path>" in code or "..." in code or "<catalog>" in code
    has_example_comment = "# Example" in code or "# Sample" in code or "// Example" in code
    has_output_comment = "# Output:" in code or "# Returns:" in code or "// Output:" in code
    
    # Determine category
    category = "NEEDS_REVIEW"
    confidence = "low"
    
    if is_shell_command:
        category = "SHELL_COMMAND"
        confidence = "high"
    elif lang == "sql":
        # SQL snippets need tables to exist
        if "CREATE TABLE" in code or "CREATE OR REPLACE" in code:
            category = "SELF_CONTAINED"
            confidence = "medium"
        elif "FROM" in code:
            category = "EXTENDS_SETUP"
            confidence = "high"
        else:
            category = "EXAMPLE_ONLY"
            confidence = "medium"
    elif has_placeholders or has_example_comment or is_short:
        category = "EXAMPLE_ONLY"
        confidence = "high"
    elif has_spark_creation and has_imports:
        category = "SELF_CONTAINED"
        confidence = "high"
    elif has_imports and (has_file_reading or has_data_creation) and has_rx_registration:
        category = "SELF_CONTAINED"
        confidence = "high"
    elif has_file_reading and has_imports:
        category = "SELF_CONTAINED"  
        confidence = "medium"  # May need data files
    elif uses_existing_df or uses_existing_rasters or uses_rx_without_import or uses_spark_without_creation:
        category = "EXTENDS_SETUP"
        confidence = "high"
    elif has_output_comment:
        category = "EXAMPLE_ONLY"
        confidence = "high"
    
    snippet["category"] = category
    snippet["confidence"] = confidence
    snippet["analysis"] = {
        "has_imports": has_imports,
        "has_spark_creation": has_spark_creation,
        "has_data_creation": has_data_creation,
        "has_file_reading": has_file_reading,
        "has_rx_registration": has_rx_registration,
        "uses_existing_df": uses_existing_df,
        "uses_existing_rasters": uses_existing_rasters,
        "uses_rx_without_import": uses_rx_without_import,
        "uses_spark_without_creation": uses_spark_without_creation,
        "has_placeholders": has_placeholders,
        "is_short": is_short
    }
    
    return snippet


def identify_common_setups(snippets: List[Dict]) -> Dict:
    """Identify common setup patterns needed across snippets"""
    setup_patterns = {
        "rasterx_basic": {
            "description": "Basic RasterX setup with registration",
            "needed_by_count": 0,
            "files": set()
        },
        "gridx_bng": {
            "description": "GridX BNG setup with registration",
            "needed_by_count": 0,
            "files": set()
        },
        "vectorx_basic": {
            "description": "VectorX setup with registration",
            "needed_by_count": 0,
            "files": set()
        },
        "raster_data_loading": {
            "description": "Load sample raster data for processing",
            "needed_by_count": 0,
            "files": set()
        },
        "vector_data_loading": {
            "description": "Load sample vector data for processing",
            "needed_by_count": 0,
            "files": set()
        }
    }
    
    for snippet in snippets:
        if snippet["category"] == "EXTENDS_SETUP":
            code = snippet["code"]
            file = snippet["source_file"]
            
            if "rx." in code or "rst_" in code:
                setup_patterns["rasterx_basic"]["needed_by_count"] += 1
                setup_patterns["rasterx_basic"]["files"].add(file)
            
            if "bx." in code or "bng_" in code:
                setup_patterns["gridx_bng"]["needed_by_count"] += 1
                setup_patterns["gridx_bng"]["files"].add(file)
            
            if "vx." in code or "st_legacy" in code:
                setup_patterns["vectorx_basic"]["needed_by_count"] += 1
                setup_patterns["vectorx_basic"]["files"].add(file)
            
            if re.search(r'\brasters\b', code) or re.search(r'\braster_df\b', code):
                setup_patterns["raster_data_loading"]["needed_by_count"] += 1
                setup_patterns["raster_data_loading"]["files"].add(file)
            
            if "shapefile" in code or "geojson" in code or re.search(r'\bshapes\b', code):
                setup_patterns["vector_data_loading"]["needed_by_count"] += 1
                setup_patterns["vector_data_loading"]["files"].add(file)
    
    # Convert sets to lists for JSON serialization
    for pattern in setup_patterns.values():
        pattern["files"] = sorted(list(pattern["files"]))
    
    return setup_patterns


def main():
    """Main inventory function"""
    if not DOCS_DIR.exists():
        print(f"Error: {DOCS_DIR} does not exist", file=sys.stderr)
        sys.exit(1)
    
    all_snippets = []
    file_summaries = []
    
    # Process all markdown files
    for md_file in sorted(DOCS_DIR.rglob("*.md")):
        blocks = extract_code_blocks(md_file)
        analyzed = [analyze_snippet(b) for b in blocks]
        
        if analyzed:
            # Summary per file
            summary = {
                "file": str(md_file.relative_to("docs/docs")),
                "total_snippets": len(analyzed),
                "by_category": {},
                "by_language": {}
            }
            
            for snippet in analyzed:
                cat = snippet["category"]
                lang = snippet["language"]
                summary["by_category"][cat] = summary["by_category"].get(cat, 0) + 1
                summary["by_language"][lang] = summary["by_language"].get(lang, 0) + 1
            
            file_summaries.append(summary)
            all_snippets.extend(analyzed)
    
    # Overall statistics
    stats = {
        "total_files": len(file_summaries),
        "total_snippets": len(all_snippets),
        "by_category": {},
        "by_language": {}
    }
    
    for snippet in all_snippets:
        cat = snippet["category"]
        lang = snippet["language"]
        stats["by_category"][cat] = stats["by_category"].get(cat, 0) + 1
        stats["by_language"][lang] = stats["by_language"].get(lang, 0) + 1
    
    # Identify common setups
    common_setups = identify_common_setups(all_snippets)
    
    # Generate inventory
    inventory = {
        "generated_at": "2026-01-12",
        "docs_directory": str(DOCS_DIR),
        "statistics": stats,
        "common_setups_needed": common_setups,
        "file_summaries": file_summaries,
        "snippets": all_snippets
    }
    
    # Write inventory
    output_file = Path("docs/doc-snippet-inventory.json")
    output_file.write_text(json.dumps(inventory, indent=2))
    
    # Print summary
    print(f"📊 Documentation Snippet Inventory")
    print(f"=" * 60)
    print(f"Total files with code: {stats['total_files']}")
    print(f"Total code snippets: {stats['total_snippets']}")
    print()
    print(f"By Category:")
    for cat, count in sorted(stats["by_category"].items(), key=lambda x: -x[1]):
        pct = count / stats['total_snippets'] * 100
        print(f"  {cat:20s}: {count:4d} ({pct:5.1f}%)")
    print()
    print(f"By Language:")
    for lang, count in sorted(stats["by_language"].items(), key=lambda x: -x[1]):
        pct = count / stats['total_snippets'] * 100
        print(f"  {lang:20s}: {count:4d} ({pct:5.1f}%)")
    print()
    print(f"Common Setups Needed:")
    for name, info in common_setups.items():
        print(f"  {name:25s}: {info['needed_by_count']:3d} snippets in {len(info['files'])} files")
    print()
    print(f"✅ Inventory saved to: {output_file}")
    print()
    print("Next steps:")
    print("1. Review snippets marked as NEEDS_REVIEW")
    print("2. Identify common setup patterns for EXTENDS_SETUP snippets")
    print("3. Add test markers to categorized snippets")


if __name__ == "__main__":
    main()
