# graph_pattern_finder

Modular pipeline for mining delay correlation patterns from any knowledge graph that follows the **ProcessCase / ProcessNode / ProcessEvent / ProcessEdge** schema.

Each stage can run **independently** (on its own inputs) or **in sequence** (chained via the orchestrator).

---

## Stages

| Stage | File | Purpose |
|-------|------|---------|
| **0** | `query.py` (project root) | Static Cypher query and Neo4j connection helpers. Edit this to point at a different graph. |
| **1** | `stage_1_extract.py` | Connects to Neo4j, runs the extraction query, returns raw case data. |
| **2** | `stage_2_discover.py` | Discovers the pipeline structure: canonical event order, transitions, blocking points, and drill-down fields. |
| **3** | `stage_3_baseline.py` | Computes median transition times (baselines), flags SLOW transitions, identifies STUCK points, and annotates every case. |
| **4** | `stage_4_tokenise.py` | Converts annotated cases into ordered token sequences (`SLOW:step`, `STUCK:point`). Optionally enriches tokens with field values. |
| **5** | `stage_5_mine.py` | Runs PrefixSpan on token sequences to find all frequent subsequences above a support threshold. |
| **6** | `stage_6_filter_rank.py` | Filters mined patterns for cause->effect correlations, computes impact metrics, ranks by `support * impact`, and runs field-enriched mining. |
| **all** | `run_pipeline.py` | Orchestrator that chains stages 1-6 and prints a full report. |

Shared utilities live in `helpers.py` (timestamp parsing, median, time formatting).

---

## Running the Full Pipeline

```bash
python -m graph_pattern_finder.run_pipeline
```

This executes all stages in order and writes `pipeline_results.json`.

---

## Running Stages Independently

Each stage reads JSON files from the previous stage and writes its own output. You can run any stage on its own as long as its input files exist.

### Stage 1: Extract cases from Neo4j

```bash
python -m graph_pattern_finder.stage_1_extract
```

**Input:** Live Neo4j database (configured via `.env` and `query.py`)
**Output:** `extracted_cases.json`

### Stage 2: Discover pipeline structure

```bash
python -m graph_pattern_finder.stage_2_discover
```

**Input:** `extracted_cases.json`
**Output:** `discovered_pipeline.json` (event order, transitions, blocking points, fields)

### Stage 3: Compute baselines and flag delays

```bash
python -m graph_pattern_finder.stage_3_baseline
```

**Input:** `extracted_cases.json` + `discovered_pipeline.json`
**Output:** `baselines.json` + `annotated_cases.json`

### Stage 4: Tokenise cases

```bash
python -m graph_pattern_finder.stage_4_tokenise
```

**Input:** `annotated_cases.json` + `discovered_pipeline.json`
**Output:** `token_sequences.json`

### Stage 5: Mine patterns with PrefixSpan

```bash
python -m graph_pattern_finder.stage_5_mine
```

**Input:** `token_sequences.json`
**Output:** `mined_patterns.json`

### Stage 6: Filter and rank correlations

```bash
python -m graph_pattern_finder.stage_6_filter_rank
```

**Input:** `mined_patterns.json` + `annotated_cases.json` + `token_sequences.json` + `discovered_pipeline.json`
**Output:** `correlation_results.json`

---

## Using Stages as a Library

Every stage exposes its core function for import. You can mix and match:

```python
# Example: run PrefixSpan on your own token sequences
from graph_pattern_finder.stage_5_mine import mine_patterns

my_sequences = [
    ["SLOW:STEP_A", "STUCK:STEP_C"],
    ["SLOW:STEP_A", "SLOW:STEP_B", "STUCK:STEP_C"],
    ["SLOW:STEP_B", "STUCK:STEP_C"],
]
results = mine_patterns(my_sequences, min_support=2)
for support, pattern in results:
    print(f"  {support}x  {' -> '.join(pattern)}")
```

```python
# Example: compute baselines on pre-extracted data
from graph_pattern_finder.stage_2_discover import discover_pipeline
from graph_pattern_finder.stage_3_baseline import compute_baselines_and_flag

cases = [...]  # your own case data in the expected dict format
order, transitions, bp_map, fields = discover_pipeline(cases)
baselines, annotated = compute_baselines_and_flag(cases, transitions, bp_map, fields)
```

---

## Data Flow

```
Neo4j DB
  |
  v
[Stage 1: Extract] --> extracted_cases.json
  |
  v
[Stage 2: Discover] --> discovered_pipeline.json
  |
  v
[Stage 3: Baseline] --> baselines.json + annotated_cases.json
  |
  v
[Stage 4: Tokenise] --> token_sequences.json
  |
  v
[Stage 5: Mine] --> mined_patterns.json
  |
  v
[Stage 6: Filter & Rank] --> correlation_results.json
```

---

## Requirements

- Python 3.10+
- `neo4j` (Python driver)
- `python-dotenv`
- `prefixspan`

Install:
```bash
pip install neo4j python-dotenv prefixspan
```

---

## Configuration

Database connection is configured via a `.env` file in the project root:

```
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
```

Hyperparameters are set at the top of `run_pipeline.py`:
- `MIN_SUPPORT_FRAC = 0.05` -- minimum fraction of cases a pattern must appear in
- `SLOW_MULTIPLIER = 3.0` -- a transition is SLOW if it exceeds 3x the median duration
