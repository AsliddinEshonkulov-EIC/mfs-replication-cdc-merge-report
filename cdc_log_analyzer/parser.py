"""Log parser for CDC merge Glue job logs."""

import csv
import re
from typing import List, Optional, Tuple

from .models import Cycle, JobRun, Phase


class LogParser:
    """Parses CDC merge log files and extracts timing information."""
    
    # Regex patterns for log markers
    PATTERNS = {
        'job_args': re.compile(r"'JOB_RUN_ID':\s*'([^']+)'.*'JOB_NAME':\s*'([^']+)'"),
        'processing_table': re.compile(r'Processing (\d+) messages for table (\w+)'),
        'setup': re.compile(r'Set up merge environment'),
        'cdc_merge': re.compile(r"Executing CDC Merge to Branch! S3 URIs: \[(.+?)\]"),
        'upsert_branch': re.compile(r'Apply upsert to branch!'),
        'dq_start': re.compile(r'Evaluating data quality\.\.\.'),
        'dq_complete': re.compile(r'Data quality evaluation completed!'),
        'fast_forward': re.compile(r'Fast forward to main was successful'),
        'merge_success': re.compile(r'Apply merge or insert for passed records'),
        'populate_an_mid': re.compile(r'Populate updated AN/MID values'),
        'cleanup': re.compile(r'Clean up environment'),
        'table_complete': re.compile(r'Quality Gate for table (\w+) completed in ([\d.]+) seconds'),
    }
    
    def parse_file(self, filepath: str) -> JobRun:
        """Parse a log file and return a JobRun object."""
        rows = self._load_csv(filepath)
        return self._parse_rows(rows)
    
    def _load_csv(self, filepath: str) -> List[Tuple[int, str]]:
        """Load CSV and return list of (timestamp, message) tuples."""
        rows = []
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ts = int(row['timestamp'])
                    msg = row['message'].strip()
                    if msg:
                        rows.append((ts, msg))
                except (ValueError, KeyError):
                    continue
        return sorted(rows, key=lambda x: x[0])
    
    def _parse_rows(self, rows: List[Tuple[int, str]]) -> JobRun:
        """Parse rows and build JobRun object."""
        job_run_id = ""
        job_name = ""
        start_ts = rows[0][0] if rows else 0
        end_ts = rows[-1][0] if rows else 0
        
        cycles: List[Cycle] = []
        current_cycle: Optional[Cycle] = None
        phase_starts: dict = {}
        
        for ts, msg in rows:
            # Extract job info
            match = self.PATTERNS['job_args'].search(msg)
            if match:
                job_run_id = match.group(1)
                job_name = match.group(2)
                continue
            
            # New table processing starts
            match = self.PATTERNS['processing_table'].search(msg)
            if match:
                # Save previous cycle if exists
                if current_cycle:
                    cycles.append(current_cycle)
                
                file_count = int(match.group(1))
                table_name = match.group(2)
                current_cycle = Cycle(
                    table_name=table_name,
                    start_ts=ts,
                    end_ts=ts,
                    file_count=file_count,
                )
                phase_starts = {}
                continue
            
            if not current_cycle:
                continue
            
            # Track phase transitions
            if self.PATTERNS['setup'].search(msg):
                phase_starts['Setup'] = ts
            
            elif self.PATTERNS['cdc_merge'].search(msg):
                # End Setup, start Read Parquet
                if 'Setup' in phase_starts:
                    current_cycle.phases['Setup'] = Phase(
                        name='Setup',
                        start_ts=phase_starts['Setup'],
                        end_ts=ts
                    )
                phase_starts['Read Parquet'] = ts
            
            elif self.PATTERNS['upsert_branch'].search(msg):
                # End Read Parquet, start Upsert to Branch
                if 'Read Parquet' in phase_starts:
                    current_cycle.phases['Read Parquet'] = Phase(
                        name='Read Parquet',
                        start_ts=phase_starts['Read Parquet'],
                        end_ts=ts
                    )
                phase_starts['Upsert to Branch'] = ts
            
            elif self.PATTERNS['dq_start'].search(msg):
                # End Upsert to Branch, start Data Quality
                if 'Upsert to Branch' in phase_starts:
                    current_cycle.phases['Upsert to Branch'] = Phase(
                        name='Upsert to Branch',
                        start_ts=phase_starts['Upsert to Branch'],
                        end_ts=ts
                    )
                phase_starts['Data Quality'] = ts
            
            elif self.PATTERNS['dq_complete'].search(msg):
                # End Data Quality, start Fast Forward
                if 'Data Quality' in phase_starts:
                    current_cycle.phases['Data Quality'] = Phase(
                        name='Data Quality',
                        start_ts=phase_starts['Data Quality'],
                        end_ts=ts
                    )
                phase_starts['Fast Forward'] = ts
            
            elif self.PATTERNS['fast_forward'].search(msg):
                current_cycle.merge_type = 'fast_forward'
            
            elif self.PATTERNS['merge_success'].search(msg):
                current_cycle.merge_type = 'merge'
            
            elif self.PATTERNS['populate_an_mid'].search(msg):
                # End Fast Forward, start Populate AN/MID
                if 'Fast Forward' in phase_starts:
                    current_cycle.phases['Fast Forward'] = Phase(
                        name='Fast Forward',
                        start_ts=phase_starts['Fast Forward'],
                        end_ts=ts
                    )
                phase_starts['Populate AN/MID'] = ts
            
            elif self.PATTERNS['cleanup'].search(msg):
                # End Populate AN/MID, start Cleanup
                if 'Populate AN/MID' in phase_starts:
                    current_cycle.phases['Populate AN/MID'] = Phase(
                        name='Populate AN/MID',
                        start_ts=phase_starts['Populate AN/MID'],
                        end_ts=ts
                    )
                phase_starts['Cleanup'] = ts
            
            elif self.PATTERNS['table_complete'].search(msg):
                # End Cleanup and cycle
                if 'Cleanup' in phase_starts:
                    current_cycle.phases['Cleanup'] = Phase(
                        name='Cleanup',
                        start_ts=phase_starts['Cleanup'],
                        end_ts=ts
                    )
                current_cycle.end_ts = ts
        
        # Don't forget the last cycle
        if current_cycle:
            cycles.append(current_cycle)
        
        return JobRun(
            job_run_id=job_run_id,
            job_name=job_name,
            start_ts=start_ts,
            end_ts=end_ts,
            cycles=cycles,
        )
