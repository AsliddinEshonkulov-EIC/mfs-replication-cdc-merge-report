"""Analyzer for computing statistics from parsed log data."""

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

from .models import PHASES, JobRun


@dataclass
class PhaseStats:
    """Statistics for a single phase."""
    name: str
    total_sec: float
    avg_sec: float
    count: int
    pct: float  # percentage of total time


@dataclass
class TableStats:
    """Statistics for a single table."""
    name: str
    total_sec: float
    cycle_count: int
    file_count: int
    phase_times: Dict[str, float]  # phase_name -> total seconds


@dataclass
class AnalysisResult:
    """Complete analysis result for a job run."""
    job_run: JobRun
    phase_stats: List[PhaseStats]  # sorted by total time desc
    table_stats: List[TableStats]  # sorted by total time desc
    total_processing_time: float


def analyze(job_run: JobRun) -> AnalysisResult:
    """Analyze a job run and compute statistics."""
    # Compute phase totals
    phase_totals: Dict[str, float] = defaultdict(float)
    phase_counts: Dict[str, int] = defaultdict(int)
    
    # Compute table totals
    table_data: Dict[str, dict] = defaultdict(
        lambda: {'total': 0.0, 'cycles': 0, 'files': 0, 'phases': defaultdict(float)}
    )
    
    for cycle in job_run.cycles:
        table = table_data[cycle.table_name]
        table['cycles'] += 1
        table['files'] += cycle.file_count
        
        cycle_total = 0.0
        for phase_name, phase in cycle.phases.items():
            duration = phase.duration_sec
            phase_totals[phase_name] += duration
            phase_counts[phase_name] += 1
            table['phases'][phase_name] += duration
            cycle_total += duration
        
        table['total'] += cycle_total
    
    # Calculate total processing time
    total_time = sum(phase_totals.values())
    
    # Build phase stats (sorted by total time desc)
    phase_stats = []
    for name in PHASES:
        total = phase_totals.get(name, 0.0)
        count = phase_counts.get(name, 0)
        phase_stats.append(PhaseStats(
            name=name,
            total_sec=total,
            avg_sec=total / count if count > 0 else 0.0,
            count=count,
            pct=(total / total_time * 100) if total_time > 0 else 0.0,
        ))
    phase_stats.sort(key=lambda x: x.total_sec, reverse=True)
    
    # Build table stats (sorted by total time desc)
    table_stats = []
    for name, data in table_data.items():
        table_stats.append(TableStats(
            name=name,
            total_sec=data['total'],
            cycle_count=data['cycles'],
            file_count=data['files'],
            phase_times=dict(data['phases']),
        ))
    table_stats.sort(key=lambda x: x.total_sec, reverse=True)
    
    return AnalysisResult(
        job_run=job_run,
        phase_stats=phase_stats,
        table_stats=table_stats,
        total_processing_time=total_time,
    )
