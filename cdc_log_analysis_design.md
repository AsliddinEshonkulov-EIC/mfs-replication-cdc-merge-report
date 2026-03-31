# CDC Merge Log Analysis - Design

## Input
- Two CSV log files (5-hour job runs each)
- Format: `timestamp` (ms epoch), `message`

---

## Phases to Track

| Phase | Start Marker | End Marker |
|-------|--------------|------------|
| Setup | `Set up merge environment` | `Executing CDC Merge to Branch` |
| Read Parquet | `Executing CDC Merge to Branch! S3 URIs:` | `Apply upsert to branch!` |
| Upsert to Branch | `Apply upsert to branch!` | `Evaluating data quality with ruleset` |
| Data Quality | `Evaluating data quality...` | `Data quality evaluation completed!` |
| Fast Forward/Merge | `Data quality evaluation completed!` | `Populate updated AN/MID values` |
| Populate AN/MID | `Populate updated AN/MID values` | `Clean up environment` |
| Cleanup | `Clean up environment` | `Quality Gate for table .* completed` |

---

## HTML Report

### Layout
```
┌────────────────────────────────────────────────────────────┐
│  CDC MERGE LOG ANALYSIS                                    │
│  [Run 1]  [Run 2]   ← tabs to switch                       │
├────────────────────────────────────────────────────────────┤
│  Job: cdl-glue-job-mfs-replication-cdc-merge               │
│  Duration: 5h 0m  |  Tables: 42  |  Cycles: 156            │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  TIME SPENT BY PHASE                                       │
│  ──────────────────────────────────────────────────────    │
│  [Horizontal bar chart - phases sorted by time]            │
│                                                            │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  PER-TABLE BREAKDOWN                                       │
│  ──────────────────────────────────────────────────────    │
│  [Stacked bar chart - tables with phase segments]          │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### Chart 1: Time Spent by Phase

Horizontal bars sorted by total time. Easy-to-read format.

```
Data Quality      ████████████████████████████  11h 3m   (73.7%)
Upsert to Branch  ████████                       1h 29m  ( 9.9%)
Populate AN/MID   ██████                            47m  ( 5.3%)
Fast Forward      █████                             37m  ( 4.2%)
Read Parquet      ██                                15m  ( 1.6%)
Cleanup           ░                                  2m  ( 0.2%)
Setup             ░                                 <1m  ( 0.0%)
```

**Hover tooltip:**
```
Data Quality
Total: 11h 3m (39,784s)
Avg per cycle: 4m 15s
```

### Chart 2: Per-Table Breakdown

Stacked horizontal bars showing phase distribution per table.

```
article        ████████████████████████████████  1h 8m   (8 cycles)
issue          ███████████████████████████████   1h 9m  (12 cycles)
artconcept_an  █████████████████████████████       59m  (45 cycles)
mag            ████████████████████                44m  (15 cycles)
artsubject_an  █████████████                       29m  (38 cycles)
...
```

**Hover on segment:**
```
issue → Data Quality
Time: 51m (73.7% of table)
Cycles: 12
```

### Time Format Rules

| Duration | Display |
|----------|---------|
| < 1 min | `45s` |
| 1-59 min | `4m 15s` |
| 1-24 hours | `1h 29m` |
| > 24 hours | `1d 2h` |

### Phase Colors
```
██ Data Quality     #e74c3c (red)
██ Upsert to Branch #3498db (blue)  
██ Populate AN/MID  #2ecc71 (green)
██ Fast Forward     #9b59b6 (purple)
██ Read Parquet     #f39c12 (orange)
██ Setup            #95a5a6 (gray)
██ Cleanup          #7f8c8d (dark gray)
```

---

## Implementation

### Files
```
cdc_log_analyzer/
├── parser.py      # Log parsing
├── analyzer.py    # Statistics
└── report.py      # HTML generation
```

### Usage
```bash
python -m cdc_log_analyzer run1.csv run2.csv -o report.html
```

---

## Edge Cases
1. Multi-line log messages (merge queries)
2. Failed cycles (missing end markers)
3. Tables without AN/MID population
