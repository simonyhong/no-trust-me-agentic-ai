# No-Trust-Me Agentic AI

No-Trust-Me Agentic AI is an experimental BRD-driven agent framework. A Business Requirement Document (BRD) describes a task, an LLM generates or repairs a Python function, and deterministic Python checks decide whether that function may be used.

The core idea is simple: do not trust the LLM's self-evaluation. The generated function must pass Python-owned checks before deployment:

1. the BRD-authored function input/output contract,
2. the BRD-authored examples and expected outputs,
3. the first `N_ITEMS_TO_PASS` labeled jobs from the matching jobs file.

After a function is deployed, normal jobs are processed by the Python function directly. Python derives the implementation function name deterministically from the BRD filename (for example, `BRD_word_count.txt -> word_count()`), so no LLM call is needed merely to initialize or re-read a BRD. The framework uses the LLM only when a new/generated implementation is actually needed or during failure recovery. A generated implementation may optionally call `my_tools.ask_gpt(...)` during ordinary jobs when its BRD truly requires LLM reasoning; in that case those jobs do invoke the LLM and their outputs may be nondeterministic.

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

This section must describe the runtime job records and contain exactly one standalone line for each trust-critical directive:

```text
ID_FIELD=incident_id
EXPECTED_FIELD=expected_word_count
N_ITEMS_TO_PASS=15
```

Python parses these directives directly; the LLM does not choose the job ID field or expected-answer field. A job is labeled when it contains `EXPECTED_FIELD`. Before deployment, a candidate implementation must pass the first `N_ITEMS_TO_PASS` labeled jobs in file order.

## Candidate selection order

The callable name for a new implementation is derived from the BRD filename by removing the `BRD_` prefix. For example, `BRD_email_router.txt` maps to `email_router()`. Existing handcrafted modules with a legacy/custom name remain usable when they contain exactly one public function.

For each current BRD hash, the worker tries candidates in this order:

1. optional handcrafted implementation from `saved_functions/<BRD stem>_handcrafted/`,
2. cached generated implementation from `saved_functions/registry.json`,
3. new GPT-generated implementation.

Every candidate must pass the same Python-owned validation: all BRD-authored tests plus the first `N_ITEMS_TO_PASS` labeled jobs.

## Runtime recovery behavior

A normal BRD content change invalidates the active in-memory function, clears any previous reflection, reparses the BRD, reruns deployment preflight, and starts normal candidate selection again.

A runtime batch failure is different. Runtime inputs are first validated against the BRD contract so malformed job data is rejected without blaming the function. If a valid job exposes a function failure, the implementation is quarantined, the failure reflection is retained, and the next recovery pass skips handcrafted/cache reuse once and goes directly to GPT generation with that reflection. After three consecutive live-batch failures for the same BRD hash, the worker enters blocked mode to stop unbounded regeneration.

## Setup

Create and activate a Python environment, then install dependencies:

```bash
pip install -r requirements.txt
```

Copy the tracked template to a private local `.env` file.

On PowerShell:

```powershell
Copy-Item .env.example .env
```

On macOS/Linux:

```bash
cp .env.example .env
```

Then edit **`.env`**, not `.env.example`, and replace the placeholder values:

```text
AZURE_OPENAI_API_KEY=your-real-key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2025-01-01-preview
AZURE_OPENAI_DEPLOYMENT=your-deployment-name
```

Use one `KEY=value` assignment per line. Quotes are not required.

The program automatically loads the repository-root `.env` file. Existing shell/environment variables take precedence over values in `.env`.

**Never put a real key in `.env.example` and never commit `.env`.** The tracked `.env.example` is only a safe template; the real `.env` file is ignored by Git.

## Run the word-count demo

```bash
python job_manager.py
```

The manager scans `documents/BRD_*.txt`, launches one worker per BRD up to the concurrency cap, and processes jobs from the matching JSON file.

Runtime state and local demonstration outputs are written to files such as:

```text
documents/done_BRD_word_count_jobs.json
documents/results_BRD_word_count_jobs.json
saved_functions/registry.json
saved_functions/BRD_word_count/
```

The results file persists successfully processed job outputs. Runtime state is flushed in bounded batches, with results written before done-state, so a crash can cause a small amount of safe re-processing but should not mark a successful job done before its output has been persisted.

Malformed runtime records are recorded under `rejected` in the matching `done_*.json` file using a hash of the record contents. If that record is corrected later, its hash changes and the corrected job becomes eligible again. These runtime-generated files are intentionally ignored by Git.

## Resetting done-state during debugging

By default, existing `done_BRD_*_jobs.json` files are preserved across restarts so processed jobs are not re-run accidentally.

For debugging only, you can request a reset on startup:

```bash
RESET_DONE_STATE=1 python job_manager.py
```

## Security notice

Generated Python code is statically screened before execution with an import allowlist plus restrictions on dangerous builtins, frame/code-object traversal, private/dunder attributes, and string-format attribute traversal. Generated code receives only a narrow `my_tools` surface rather than the raw authenticated Azure client. However, generated Python still executes inside the worker process:

```python
exec(code, ns)
```

The static screen is defense-in-depth, not a sandbox or security boundary. Run this project only in a controlled environment. Do not expose the generation path to untrusted BRDs, untrusted model output, production credentials, or sensitive data until generated implementations are isolated in a separate process with execution timeouts and stronger filesystem/network controls.

## Public-data caution

The example files in `documents/` are sanitized. The `.gitignore` intentionally whitelists only the word-count demonstration so real BRDs or real jobs are not accidentally committed.
