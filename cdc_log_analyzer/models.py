"""Data models for CDC log analysis."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Phase:
    """Timing for a single phase within a cycle."""
    name: str
    start_ts: int  # milliseconds
    end_ts: int    # milliseconds
    
    @property
    def duration_ms(self) -> int:
        return self.end_ts - self.start_ts
    
    @property
    def duration_sec(self) -> float:
        return self.duration_ms / 1000.0


@dataclass
class Cycle:
    """A single table processing cycle."""
    table_name: str
    start_ts: int
    end_ts: int
    file_count: int
    phases: Dict[str, Phase] = field(default_factory=dict)
    merge_type: str = "unknown"  # 'fast_forward' or 'merge'
    
    @property
    def duration_sec(self) -> float:
        return (self.end_ts - self.start_ts) / 1000.0
    
    def phase_duration(self, phase_name: str) -> float:
        """Get duration in seconds for a phase, or 0 if not found."""
        if phase_name in self.phases:
            return self.phases[phase_name].duration_sec
        return 0.0


@dataclass
class JobRun:
    """A complete job run with all cycles."""
    job_run_id: str
    job_name: str
    start_ts: int
    end_ts: int
    cycles: List[Cycle] = field(default_factory=list)
    
    @property
    def duration_sec(self) -> float:
        return (self.end_ts - self.start_ts) / 1000.0
    
    @property
    def table_count(self) -> int:
        return len(set(c.table_name for c in self.cycles))
    
    @property
    def total_files(self) -> int:
        return sum(c.file_count for c in self.cycles)


# Phase names (in typical execution order)
PHASES = [
    "Setup",
    "Read Parquet",
    "Upsert to Branch",
    "Data Quality",
    "Fast Forward",
    "Populate AN/MID",
    "Cleanup",
]

# Phase colors for HTML report
PHASE_COLORS = {
    "Data Quality": "#e74c3c",      # red
    "Upsert to Branch": "#3498db",  # blue
    "Populate AN/MID": "#2ecc71",   # green
    "Fast Forward": "#9b59b6",      # purple
    "Read Parquet": "#f39c12",      # orange
    "Setup": "#95a5a6",             # gray
    "Cleanup": "#7f8c8d",           # dark gray
}
