#!/bin/bash
# gbx:coverage:gaps - Analyze coverage gaps and identify improvement opportunities

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📊 GeoBrix: Coverage Gaps Analysis"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:coverage:gaps${NC} ${YELLOW}<language>${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Languages:${NC}"
    echo -e "  ${GREEN}scala${NC}    Analyze Scala coverage (uses target/scoverage.xml)"
    echo -e "  ${GREEN}python${NC}   Analyze Python coverage (uses coverage.xml)"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--threshold <percent>${NC}  Highlight packages below threshold (default: 90)"
    echo -e "  ${GREEN}--verbose${NC}              Show file-level details"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:coverage:gaps scala${NC}"
    echo -e "  ${YELLOW}gbx:coverage:gaps scala --threshold 85${NC}"
    echo -e "  ${YELLOW}gbx:coverage:gaps python --verbose${NC}"
    echo ""
}

# Parse arguments
LANGUAGE=""
THRESHOLD="90"
VERBOSE=false

# First argument should be language
if [[ $# -gt 0 ]] && [[ ! "$1" =~ ^-- ]]; then
    LANGUAGE="$1"
    shift
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
done

# Validate language
if [ -z "$LANGUAGE" ]; then
    echo -e "${RED}❌ Error: Language required (scala or python)${NC}"
    echo ""
    show_help
    exit 1
fi

if [ "$LANGUAGE" != "scala" ] && [ "$LANGUAGE" != "python" ]; then
    echo -e "${RED}❌ Error: Invalid language '$LANGUAGE' (must be 'scala' or 'python')${NC}"
    exit 1
fi

cd "$PROJECT_ROOT"

show_banner "📊 GeoBrix: Coverage Gaps Analysis"

# Scala coverage analysis
if [ "$LANGUAGE" = "scala" ]; then
    COVERAGE_FILE="target/scoverage.xml"
    
    if [ ! -f "$COVERAGE_FILE" ]; then
        echo -e "${RED}❌ Error: Coverage data not found${NC}"
        echo ""
        echo -e "${YELLOW}Run coverage first:${NC}"
        echo -e "  ${GREEN}gbx:coverage:scala${NC}              # Full coverage"
        echo -e "  ${GREEN}gbx:coverage:scala-package <pkg>${NC} # Package-only"
        echo ""
        exit 1
    fi
    
    echo -e "${CYAN}📄 Coverage file: ${YELLOW}$COVERAGE_FILE${NC}"
    echo -e "${CYAN}🎯 Threshold: ${YELLOW}≥${THRESHOLD}%${NC}"
    echo ""
    show_separator
    
    # Parse coverage data using Python (more reliable than bash XML parsing)
    python3 - <<EOF
import xml.etree.ElementTree as ET
from collections import defaultdict
import sys

# Parse coverage XML
tree = ET.parse("$COVERAGE_FILE")
root = tree.getroot()

# Extract package coverage
packages = defaultdict(lambda: {"statements": 0, "covered": 0, "files": []})

for pkg in root.findall('.//package'):
    pkg_name = pkg.get('name', 'unknown')
    
    # Skip test packages
    if 'test' in pkg_name.lower():
        continue
    
    # Extract package name (e.g., com.databricks.labs.gbx.rasterx)
    # We want the last component (rasterx, gridx, etc.)
    parts = pkg_name.split('.')
    if len(parts) >= 5 and parts[0] == 'com' and parts[1] == 'databricks':
        short_name = parts[4]  # rasterx, gridx, vectorx, etc.
        if len(parts) > 5:
            short_name = parts[4] + '.' + parts[5]  # gridx.bng, etc.
    else:
        short_name = pkg_name
    
    # Get statistics
    statements = int(pkg.get('statement-count', 0))
    covered = int(pkg.get('statements-invoked', 0))
    
    packages[short_name]["statements"] += statements
    packages[short_name]["covered"] += covered

# Calculate coverage percentages
results = []
for pkg_name, data in packages.items():
    if data["statements"] > 0:
        coverage = (data["covered"] / data["statements"]) * 100
        results.append({
            "name": pkg_name,
            "coverage": coverage,
            "statements": data["statements"],
            "covered": data["covered"],
            "uncovered": data["statements"] - data["covered"]
        })

# Sort by coverage (lowest first)
results.sort(key=lambda x: x["coverage"])

# Display results
threshold = float($THRESHOLD)
print("\n${CYAN}📦 Package Coverage Summary:${NC}\n")
print(f"{'Package':<20} {'Coverage':<12} {'Covered/Total':<20} {'Status'}")
print("─" * 70)

below_threshold = []
for pkg in results:
    status = "✅" if pkg["coverage"] >= threshold else "⚠️"
    color = "\033[0;32m" if pkg["coverage"] >= threshold else "\033[1;33m"
    reset = "\033[0m"
    
    print(f"{pkg['name']:<20} {color}{pkg['coverage']:>6.1f}%{reset}      "
          f"{pkg['covered']:>5}/{pkg['statements']:<5} statements   {status}")
    
    if pkg["coverage"] < threshold:
        below_threshold.append(pkg)

# Overall statistics
total_statements = sum(p["statements"] for p in results)
total_covered = sum(p["covered"] for p in results)
overall_coverage = (total_covered / total_statements * 100) if total_statements > 0 else 0

print("─" * 70)
print(f"{'Overall':<20} {overall_coverage:>6.1f}%      "
      f"{total_covered:>5}/{total_statements:<5} statements")

# Recommendations
if below_threshold:
    print("\n${YELLOW}⚠️  Packages Below Threshold (${THRESHOLD}%):${NC}\n")
    for pkg in below_threshold:
        gap = threshold - pkg["coverage"]
        statements_needed = int((threshold/100 * pkg["statements"]) - pkg["covered"])
        print(f"  ${RED}•${NC} ${YELLOW}{pkg['name']}${NC}: {pkg['coverage']:.1f}% "
              f"(need {statements_needed} more statements covered, {gap:.1f}% gap)")
    
    print(f"\n${CYAN}💡 Recommended Actions:${NC}\n")
    print(f"  1. Target lowest package: ${GREEN}{below_threshold[0]['name']}${NC}")
    print(f"     ${YELLOW}gbx:coverage:scala-package {below_threshold[0]['name']} --open${NC}\n")
    print(f"  2. Add tests to cover {below_threshold[0]['uncovered']} uncovered statements\n")
    print(f"  3. Re-run package coverage to validate improvement\n")
else:
    print(f"\n${GREEN}✅ All packages meet threshold! (≥${THRESHOLD}%)${NC}\n")

sys.exit(0)
EOF

elif [ "$LANGUAGE" = "python" ]; then
    # Python coverage analysis
    COVERAGE_FILE="python/coverage-report/coverage.xml"
    
    if [ ! -f "$COVERAGE_FILE" ]; then
        COVERAGE_FILE="docs/tests/coverage-report/coverage.xml"
    fi
    
    if [ ! -f "$COVERAGE_FILE" ]; then
        echo -e "${RED}❌ Error: Python coverage data not found${NC}"
        echo ""
        echo -e "${YELLOW}Run coverage first:${NC}"
        echo -e "  ${GREEN}gbx:coverage:python${NC}      # Unit tests"
        echo -e "  ${GREEN}gbx:coverage:python-docs${NC} # Docs tests"
        echo ""
        exit 1
    fi
    
    echo -e "${CYAN}📄 Coverage file: ${YELLOW}$COVERAGE_FILE${NC}"
    echo -e "${CYAN}🎯 Threshold: ${YELLOW}≥${THRESHOLD}%${NC}"
    echo ""
    show_separator
    
    # Python coverage parsing (simpler structure)
    python3 - <<EOF
import xml.etree.ElementTree as ET
from collections import defaultdict
import sys

try:
    tree = ET.parse("$COVERAGE_FILE")
    root = tree.getroot()
except Exception as e:
    print(f"Error parsing coverage file: {e}")
    sys.exit(1)

# Extract module coverage
modules = defaultdict(lambda: {"lines": 0, "covered": 0})

for pkg in root.findall('.//package'):
    pkg_name = pkg.get('name', 'unknown')
    
    # Skip test packages
    if 'test' in pkg_name:
        continue
    
    # Get module name (first part after geobrix)
    parts = pkg_name.split('.')
    if 'gbx' in parts:
        idx = parts.index('gbx')
        if idx + 1 < len(parts):
            module = parts[idx + 1]
        else:
            module = pkg_name
    else:
        module = pkg_name
    
    # Aggregate statistics
    for cls in pkg.findall('.//class'):
        lines = int(cls.get('line-count', 0))
        covered_lines = int(cls.get('lines-covered', 0))
        
        modules[module]["lines"] += lines
        modules[module]["covered"] += covered_lines

# Calculate and display
results = []
for module, data in modules.items():
    if data["lines"] > 0:
        coverage = (data["covered"] / data["lines"]) * 100
        results.append({
            "name": module,
            "coverage": coverage,
            "lines": data["lines"],
            "covered": data["covered"]
        })

results.sort(key=lambda x: x["coverage"])

threshold = float($THRESHOLD)
print("\n${CYAN}📦 Module Coverage Summary:${NC}\n")
print(f"{'Module':<20} {'Coverage':<12} {'Covered/Total':<20} {'Status'}")
print("─" * 70)

below_threshold = []
for mod in results:
    status = "✅" if mod["coverage"] >= threshold else "⚠️"
    color = "\033[0;32m" if mod["coverage"] >= threshold else "\033[1;33m"
    reset = "\033[0m"
    
    print(f"{mod['name']:<20} {color}{mod['coverage']:>6.1f}%{reset}      "
          f"{mod['covered']:>5}/{mod['lines']:<5} lines         {status}")
    
    if mod["coverage"] < threshold:
        below_threshold.append(mod)

total_lines = sum(m["lines"] for m in results)
total_covered = sum(m["covered"] for m in results)
overall = (total_covered / total_lines * 100) if total_lines > 0 else 0

print("─" * 70)
print(f"{'Overall':<20} {overall:>6.1f}%      {total_covered:>5}/{total_lines:<5} lines")

if below_threshold:
    print("\n${YELLOW}⚠️  Modules Below Threshold (${THRESHOLD}%):${NC}\n")
    for mod in below_threshold:
        gap = threshold - mod["coverage"]
        print(f"  ${RED}•${NC} ${YELLOW}{mod['name']}${NC}: {mod['coverage']:.1f}% (gap: {gap:.1f}%)")
    
    print(f"\n${CYAN}💡 Recommended Action:${NC}")
    print(f"  ${YELLOW}gbx:coverage:python --open${NC} (Python coverage is fast, ~30 sec)\n")
else:
    print(f"\n${GREEN}✅ All modules meet threshold! (≥${THRESHOLD}%)${NC}\n")

sys.exit(0)
EOF

fi

show_separator
echo ""
echo -e "${CYAN}💡 Tips:${NC}"
echo -e "  ${YELLOW}• Use --threshold <N> to adjust warning level${NC}"
echo -e "  ${YELLOW}• Run 'gbx:coverage:scala-package <pkg>' to target specific package${NC}"
echo -e "  ${YELLOW}• Run 'gbx:coverage:scala --report-only --open' to view full report${NC}"
echo ""
