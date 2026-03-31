"""HTML report generator for CDC log analysis."""

import json
from typing import List

from .analyzer import AnalysisResult
from .models import PHASE_COLORS, PHASES


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s" if secs > 0 else f"{mins}m"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m" if mins > 0 else f"{hours}h"


def generate_html_report(results: List[AnalysisResult], output_path: str) -> None:
    """Generate an HTML report from analysis results."""
    html = _build_html(results)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)


def _build_html(results: List[AnalysisResult]) -> str:
    """Build the complete HTML document."""
    # Prepare data for each run
    runs_data = []
    for i, result in enumerate(results):
        run_id = result.job_run.job_run_id
        
        runs_data.append({
            'id': f'run{i+1}',
            'label': run_id or f'Run {i+1}',
            'job_run_id': run_id,
            'job_name': result.job_run.job_name,
            'duration': format_duration(result.job_run.duration_sec),
            'tables': result.job_run.table_count,
            'cycles': len(result.job_run.cycles),
            'files': result.job_run.total_files,
            'phase_stats': result.phase_stats,
            'table_stats': result.table_stats,
            'total_time': result.total_processing_time,
        })
    
    # Build tabs HTML
    tabs_html = '\n'.join([
        f'<button class="tab-btn {"active" if i == 0 else ""}" onclick="showRun(\'{r["id"]}\')">{r["label"]}</button>'
        for i, r in enumerate(runs_data)
    ])
    
    # Build content for each run
    content_html = '\n'.join([_build_run_content(r, i == 0) for i, r in enumerate(runs_data)])
    
    # Build chart data as JSON
    chart_data = json.dumps({
        r['id']: _prepare_chart_data(r) for r in runs_data
    })
    
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CDC Merge Log Analysis</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        h1 {{ font-size: 1.5rem; font-weight: 600; margin-bottom: 20px; }}
        h2 {{ font-size: 1.1rem; font-weight: 600; color: #555; margin-bottom: 15px; }}
        .tabs {{ display: flex; gap: 10px; margin-bottom: 20px; }}
        .tab-btn {{ padding: 10px 20px; border: none; background: #e0e0e0; border-radius: 6px; cursor: pointer; font-size: 0.9rem; }}
        .tab-btn.active {{ background: #3498db; color: white; }}
        .tab-btn:hover {{ background: #bbb; }}
        .tab-btn.active:hover {{ background: #2980b9; }}
        .run-content {{ display: none; }}
        .run-content.active {{ display: block; }}
        .card {{ background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        .summary {{ display: flex; flex-wrap: wrap; gap: 20px; align-items: center; }}
        .summary-item {{ font-size: 0.9rem; }}
        .summary-item strong {{ color: #333; }}
        .summary-item span {{ color: #666; }}
        .chart-container {{ position: relative; height: 300px; }}
        .chart-container.tall {{ height: 500px; }}
        .legend {{ display: flex; flex-wrap: wrap; gap: 15px; margin-top: 15px; font-size: 0.8rem; }}
        .legend-item {{ display: flex; align-items: center; gap: 5px; }}
        .legend-color {{ width: 12px; height: 12px; border-radius: 2px; }}
        .subtitle {{ font-size: 0.85rem; color: #888; margin-bottom: 15px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>CDC Merge Log Analysis</h1>
        <div class="tabs">{tabs_html}</div>
        {content_html}
    </div>
    
    <script>
        const chartData = {chart_data};
        const phaseColors = {json.dumps(PHASE_COLORS)};
        const charts = {{}};
        
        function showRun(runId) {{
            document.querySelectorAll('.run-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
            document.getElementById(runId).classList.add('active');
            event.target.classList.add('active');
            
            // Initialize charts for this run if not already done
            if (!charts[runId]) {{
                initCharts(runId);
            }}
        }}
        
        function initCharts(runId) {{
            const data = chartData[runId];
            
            // Phase chart
            const phaseCtx = document.getElementById(runId + '-phase-chart').getContext('2d');
            charts[runId + '-phase'] = new Chart(phaseCtx, {{
                type: 'bar',
                data: {{
                    labels: data.phases.map(p => p.name),
                    datasets: [{{
                        data: data.phases.map(p => p.total),
                        backgroundColor: data.phases.map(p => phaseColors[p.name] || '#999'),
                        borderRadius: 4,
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: (ctx) => {{
                                    const p = data.phases[ctx.dataIndex];
                                    return [
                                        `Total: ${{p.formatted}} (${{p.pct.toFixed(1)}}%)`,
                                        `Avg/cycle: ${{p.avg_formatted}}`
                                    ];
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{ 
                            title: {{ display: true, text: 'Time (seconds)' }},
                            grid: {{ color: '#eee' }}
                        }},
                        y: {{ grid: {{ display: false }} }}
                    }}
                }}
            }});
            
            // Table chart
            const tableCtx = document.getElementById(runId + '-table-chart').getContext('2d');
            const phases = {json.dumps(PHASES)};
            charts[runId + '-table'] = new Chart(tableCtx, {{
                type: 'bar',
                data: {{
                    labels: data.tables.map(t => t.name + ' (' + t.cycles + ')'),
                    datasets: phases.map(phase => ({{
                        label: phase,
                        data: data.tables.map(t => t.phases[phase] || 0),
                        backgroundColor: phaseColors[phase] || '#999',
                    }}))
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: (ctx) => {{
                                    const t = data.tables[ctx.dataIndex];
                                    const phase = ctx.dataset.label;
                                    const val = t.phases[phase] || 0;
                                    const pct = t.total > 0 ? (val / t.total * 100).toFixed(1) : 0;
                                    return `${{phase}}: ${{formatDuration(val)}} (${{pct}}%)`;
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        x: {{ 
                            stacked: true,
                            title: {{ display: true, text: 'Time (seconds)' }},
                            grid: {{ color: '#eee' }}
                        }},
                        y: {{ 
                            stacked: true,
                            grid: {{ display: false }}
                        }}
                    }}
                }}
            }});
        }}
        
        function formatDuration(sec) {{
            if (sec < 60) return Math.round(sec) + 's';
            if (sec < 3600) {{
                const m = Math.floor(sec / 60);
                const s = Math.round(sec % 60);
                return s > 0 ? m + 'm ' + s + 's' : m + 'm';
            }}
            const h = Math.floor(sec / 3600);
            const m = Math.round((sec % 3600) / 60);
            return m > 0 ? h + 'h ' + m + 'm' : h + 'h';
        }}
        
        // Initialize first run
        document.addEventListener('DOMContentLoaded', () => {{
            const firstRunId = Object.keys(chartData)[0];
            if (firstRunId) initCharts(firstRunId);
        }});
    </script>
</body>
</html>'''


def _build_run_content(run: dict, active: bool) -> str:
    """Build HTML content for a single run."""
    legend_html = '\n'.join([
        f'<div class="legend-item"><div class="legend-color" style="background:{PHASE_COLORS.get(p, "#999")}"></div>{p}</div>'
        for p in PHASES
    ])
    
    return f'''
    <div id="{run['id']}" class="run-content {'active' if active else ''}">
        <div class="card">
            <div class="summary">
                <div class="summary-item"><strong>Job:</strong> <span>{run['job_name']}</span></div>
                <div class="summary-item"><strong>Duration:</strong> <span>{run['duration']}</span></div>
                <div class="summary-item"><strong>Tables:</strong> <span>{run['tables']}</span></div>
                <div class="summary-item"><strong>Cycles:</strong> <span>{run['cycles']}</span></div>
                <div class="summary-item"><strong>Files:</strong> <span>{run['files']}</span></div>
            </div>
        </div>
        
        <div class="card">
            <h2>Time Spent by Phase</h2>
            <p class="subtitle">Where is the job spending its time? Sorted by total time.</p>
            <div class="chart-container">
                <canvas id="{run['id']}-phase-chart"></canvas>
            </div>
        </div>
        
        <div class="card">
            <h2>Per-Table Breakdown</h2>
            <p class="subtitle">Each bar shows phase distribution. Hover for details.</p>
            <div class="chart-container tall">
                <canvas id="{run['id']}-table-chart"></canvas>
            </div>
            <div class="legend">{legend_html}</div>
        </div>
    </div>'''


def _prepare_chart_data(run: dict) -> dict:
    """Prepare chart data for JavaScript."""
    return {
        'phases': [
            {
                'name': p.name,
                'total': p.total_sec,
                'formatted': format_duration(p.total_sec),
                'avg_formatted': format_duration(p.avg_sec),
                'pct': p.pct,
            }
            for p in run['phase_stats'] if p.total_sec > 0
        ],
        'tables': [
            {
                'name': t.name,
                'total': t.total_sec,
                'cycles': t.cycle_count,
                'phases': t.phase_times,
            }
            for t in run['table_stats']
        ],
    }
