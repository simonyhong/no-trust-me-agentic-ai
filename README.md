# No-Trust-Me Agentic AI

No-Trust-Me Agentic AI is an experimental BRD-driven agent framework. A Business Requirement Document (BRD) describes a task, an LLM generates or repairs a Python function, and deterministic Python checks decide whether that function may be used.

The core idea is simple: do not trust the LLM's self-evaluation. The generated function must pass Python-owned checks before deployment:

1. the BRD-authored function input/output contract,
2. the BRD-authored examples and expected outputs,
3. the first `N_ITEMS_TO_PASS` labeled jobs from the matching jobs file.

After a function is deployed, normal jobs are processed by the Python function directly. The LLM is used during BRD initialization/change, function generation, and failure recovery, not for every ordinary job.

## Repository layout

```text
no-trust-me-agentic-ai/
├── job_manager.py                         # Main entry point
├── source/
│   ├── __init__.py
│   └── brd_processor.py                   # BRD-processing agent logic
├── documents/
│   ├── BRD_word_count.txt                 # Sanitized demonstration BRD
│   └── BRD_word_count_jobs.json           # Labeled demonstration jobs
├── docs/
│   └── No_Trust_Me_Agentic_AI.pptx        # Project overview presentation
├── saved_functions/
│   └── .gitkeep                           # Runtime generated files go here
├── .env.example
├── .gitignore
├── requirements.txt
└── LICENSE
```

`documents/` is intentionally used for the example files because this is the real runtime path. `job_manager.py` scans `documents/BRD_*.txt` and each worker expects the matching jobs file beside it as `<BRD stem>_jobs.json`.

## BRD format

Only a few BRD sections are rigid for Python inspection.

### FUNCTION INPUT/OUTPUT CONTRACT

This section must contain one JSON object with singular key `input` and key `output`:

```json
{
  "input": [
    {
      "name": "text",
      "type": "string"
    }
  ],
  "output": {
    "type": "integer"
  }
}
```

Old plural `inputs` is intentionally not the current format.

### TEST EXAMPLES & EXPECTED RESULTS

This section must contain a JSON array of user-authored tests:

```json
[
  {
    "input": "Hello world",
    "output": 2
  }
]
```

Python parses these tests directly. The code-generating LLM must not invent, rewrite, or reinterpret the test examples or expected outputs.

### JOBS DATA STRUCTURE

This section must describe the runtime job records and contain exactly one standalone deployment-gate line:

```text
N_ITEMS_TO_PASS=15
```

A job is labeled when it contains the BRD-specific expected-output field. Before deployment, a candidate implementation must pass the first `N_ITEMS_TO_PASS` labeled jobs in file order.

## Candidate selection order

For each current BRD hash, the worker tries candidates in this order:

1. optional handcrafted implementation from `saved_functions/<BRD stem>_handcrafted/`,
2. cached generated implementation from `saved_functions/registry.json`,
3. new GPT-generated implementation.

Every candidate must pass the same Python-owned validation: all BRD-authored tests plus the first `N_ITEMS_TO_PASS` labeled jobs.

## Runtime recovery behavior

A normal BRD content change invalidates the active in-memory function, clears any previous reflection, reparses the BRD, reruns deployment preflight, and starts normal candidate selection again.

A runtime batch failure is different. The failing implementation is quarantined, the failure reflection is retained, and the next recovery pass skips handcrafted/cache reuse once and goes directly to GPT generation with that reflection.

## Setup

Create and activate a Python environment, then install dependencies:

```bash
pip install -r requirements.txt
```

Copy the environment template:

```bash
cp .env.example .env
```

Set these variables in your shell or environment manager:

```bash
export AZURE_OPENAI_API_KEY="..."
export AZURE_OPENAI_ENDPOINT="https://your-resource.openai.azure.com/"
export AZURE_OPENAI_API_VERSION="2025-01-01-preview"
export AZURE_OPENAI_DEPLOYMENT="your-deployment-name"
```

This project does not load `.env` automatically; `.env.example` is only a template. Either export the variables in your shell or add your preferred environment-loading mechanism.

## Run the word-count demo

```bash
python job_manager.py
```

The manager scans `documents/BRD_*.txt`, launches one worker per BRD up to the concurrency cap, and processes jobs from the matching JSON file.

Runtime state is written to files such as:

```text
documents/done_BRD_word_count_jobs.json
saved_functions/registry.json
saved_functions/BRD_word_count/
```

Those runtime-generated files are intentionally ignored by Git.

## Resetting done-state during debugging

By default, existing `done_BRD_*_jobs.json` files are preserved across restarts so processed jobs are not re-run accidentally.

For debugging only, you can request a reset on startup:

```bash
RESET_DONE_STATE=1 python job_manager.py
```

## Security notice

Generated Python code currently executes inside the worker process without a sandbox:

```python
exec(code, ns)
```

Run this project only in a controlled environment. Do not expose the generation path to untrusted BRDs, untrusted model output, production credentials, or sensitive data until process isolation, timeouts, and stronger sandboxing are added.

## Public-data caution

The example files in `documents/` are sanitized. The `.gitignore` intentionally whitelists only the word-count demonstration so real BRDs or real jobs are not accidentally committed.
