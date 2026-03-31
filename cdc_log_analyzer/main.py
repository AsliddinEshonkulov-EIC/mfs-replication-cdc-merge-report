"""Main entry point for CDC log analyzer."""

import argparse
import sys
from pathlib import Path

# Support running both as module and directly
try:
    from .analyzer import analyze
    from .parser import LogParser
    from .report import generate_html_report
except ImportError:
    from analyzer import analyze
    from parser import LogParser
    from report import generate_html_report


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Analyze CDC merge Glue job logs and generate HTML report.'
    )
    parser.add_argument(
        'logs',
        nargs='+',
        help='Log file(s) to analyze (CSV format)'
    )
    parser.add_argument(
        '-o', '--output',
        default='report.html',
        help='Output HTML file (default: report.html)'
    )
    
    args = parser.parse_args()
    
    # Validate input files
    log_files = []
    for log_path in args.logs:
        path = Path(log_path)
        if not path.exists():
            print(f"Error: File not found: {log_path}", file=sys.stderr)
            sys.exit(1)
        log_files.append(path)
    
    # Parse and analyze each log file
    log_parser = LogParser()
    results = []
    
    for log_file in log_files:
        print(f"Parsing: {log_file}")
        job_run = log_parser.parse_file(str(log_file))
        print(f"  Found {len(job_run.cycles)} cycles across {job_run.table_count} tables")
        
        result = analyze(job_run)
        results.append(result)
    
    # Generate report
    output_path = Path(args.output)
    print(f"Generating report: {output_path}")
    generate_html_report(results, str(output_path))
    print("Done!")


if __name__ == '__main__':
    main()
