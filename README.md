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

## Blind real-job deployment gate

The code-generation LLM may be shown detailed failures from **BRD-authored examples** and may retry up to `MAX_ATTEMPTS` times. The N labeled real jobs are an independent **blind deployment gate**: after a GPT-generated candidate passes BRD examples, Python evaluates it against those jobs. A failed N-job check immediately blocks that BRD hash and requests human review; it does **not** send real-job inputs, expected outputs, actual outputs, or failing-case feedback to GPT, and it does **not** trigger another GPT generation attempt. Human-visible logs may include failure details.

Handcrafted and previously cached candidates are checked using the same BRD examples and N jobs. If such a candidate fails, Python may try the next candidate without supplying its N-job failure details to GPT. The GPT-generated candidate's first failed N-job validation is terminal for that BRD hash until the BRD changes.

## Runtime failure and human escalation

A normal BRD content change invalidates the active in-memory function, clears previous reflections, reparses the BRD, and reruns normal candidate selection and validation.

**Malformed job records** are rejected and recorded in done-state without blaming the function.

**A failure on a valid live job** (exception, wrong labeled answer, or BRD output-contract violation) now causes immediate quarantine of the active function and a persistent human-review block for that exact BRD hash. The worker stops processing that BRD and **does not ask GPT to regenerate a replacement**. The detailed failure remains in the logs for investigation; other BRDs continue operating.

**An unexpected worker-process exit** is detected by `JobManager`. The manager conservatively blocks the last launched BRD hash and quarantines its last reported active function when one is known. A nonzero worker exit is not necessarily caused by the generated function; it still needs human diagnosis. Intentional stop requests and deleted BRDs do not trigger this block.

Human-review blocks are stored in `saved_functions/registry.json` under `runtime_block`, so restarting the manager does not silently allow another attempt for the same BRD content. After investigating, edit the BRD to create a new hash (which triggers fresh validation), or explicitly clear the current hash's `runtime_block` in the registry while the manager is stopped after approving an alternative implementation. Editing only the generated/handcrafted Python file does not clear the persistent block.

**Function execution watchdog:** The manager now records when a BRD example, deployment-validation check, live job, or generated-code load executes function code. If cumulative **non-GPT computation time** exceeds `FUNCTION_CALL_TIMEOUT_SECONDS` (default **120 seconds**, set to `0` to disable), JobManager terminates the worker, blocks the BRD hash, and escalates for human review. Time spent inside the approved `my_tools.ask_gpt()` wrapper is **paused and excluded**, without erasing computation time accrued before the call. Code-generation API calls and ordinary polling are also excluded. This is a reliability watchdog, **not a security sandbox**.

**Separate GPT tool timeouts:** `GPT_TOOL_TIMEOUT_SECONDS` (default **180**) bounds the overall GPT interaction including acquiring a slot, individual requests, retries and backoff; `GPT_REQUEST_TIMEOUT_SECONDS` (default **60**) bounds each API request; and `GPT_SEMAPHORE_WAIT_TIMEOUT_SECONDS` (default **60**) limits waiting for a free GPT slot. These are separate from the function-computation budget. A GPT outage/timeout in `my_tools.ask_gpt()` is treated as an **external-service failure**: the worker logs and retries on a future polling cycle without quarantining the active function. Generation API outages likewise do not use up the 10 code-repair attempts. The SDK's internal retry mechanism is disabled so the framework owns its retry budget.

**Remaining semaphore caveat:** A worker forcibly terminated by an unrelated shutdown or crash while it holds a shared GPT semaphore permit can still strand that permit until the manager is restarted. Pausing the watchdog removes the routine false-timeout cause, but robust protection from arbitrary forced kills requires a parent-owned GPT broker or equivalent redesign.

**Logging resilience:** Each BRD worker now writes to its own logging queue; manager escalation messages go directly to the log handlers. Shutdown waits at most three seconds for each worker log stream so a hard-crashed worker cannot indefinitely block Ctrl+C or suppress the manager's logs.

**Temporary jobs-file failures:** If `BRD_*_jobs.json` is missing, incomplete or invalid during live processing, the worker logs a warning and retries next polling cycle **without** quarantining the implementation. Job-producing agents should still use atomic file replacement (`write temporary JSON -> os.replace`) to avoid partial reads.

**Remaining limitation:** The watchdog can terminate a stuck worker, but it does not restrict arbitrary filesystem/network access by a generated function. Resource isolation remains a separate future sandboxing task.

**Scope of blindness:** The code-**generating** LLM does not receive real-job test feedback. If a BRD deliberately requires an LLM-powered function (through `my_tools.ask_gpt`), that runtime LLM may still receive individual job inputs to perform its authorized task; it is not provided the expected test answers by the validator.

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

Runtime state is written to files such as:

```text
documents/done_BRD_word_count_jobs.json
saved_functions/registry.json
saved_functions/BRD_word_count/
```

**Job answers are logged only.** The program does not create separate results JSON files. If you need historical job answers, retain the logs or send outputs to an external destination before relying on this mode. Output values are not recoverable from the `done_*.json` state.

The `done_*.json` file tracks processed job IDs and rejected-record hashes. The worker flushes state after bounded batches of 50 successful jobs and on batch completion or failure; a crash may cause a small amount of reprocessing since the last flush. Malformed runtime records are recorded under `rejected` using a hash of their content; correcting a record changes its hash and makes it eligible again. These runtime-generated files are intentionally ignored by Git.

## Resetting done-state during debugging

By default, existing `done_BRD_*_jobs.json` files are preserved across restarts so processed jobs are not re-run accidentally.

For debugging only, you can request a reset on startup:

```bash
RESET_DONE_STATE=1 python job_manager.py
```

## Trusted generated-code capabilities

Generated functions cannot use `open()` or import `openpyxl` directly. When a BRD legitimately requires a local Excel workbook, the runtime exposes narrowly scoped read-only helpers through `my_tools`:

```python
my_tools.read_excel_rows("./documents/example.xlsx", "Sheet1")
my_tools.file_modified_time("./documents/example.xlsx")
my_tools.monotonic_time()
```

`read_excel_rows()` only accepts existing `.xlsx`/`.xlsm` files whose resolved path stays under the repository's `documents/` directory. It opens workbooks read-only with `data_only=True`, so generated code can use cached/calculated Excel values without receiving arbitrary filesystem access. `file_modified_time()` is restricted by the same path rules and supports BRD-defined cache refresh logic.

## Security notice

Generated Python code is statically screened before execution with an import allowlist plus restrictions on dangerous builtins, frame/code-object traversal, private/dunder attributes, and string-format attribute traversal. Generated code receives only a narrow `my_tools` surface rather than the raw authenticated Azure client or unrestricted filesystem access. However, generated Python still executes inside the worker process:

```python
exec(code, ns)
```

The static screen is defense-in-depth, not a sandbox or security boundary. Run this project only in a controlled environment. Do not expose the generation path to untrusted BRDs, untrusted model output, production credentials, or sensitive data until generated implementations are isolated in a separate process with execution timeouts and stronger filesystem/network controls.

## Public-data caution

The example files in `documents/` are sanitized. The `.gitignore` intentionally whitelists only the word-count demonstration so real BRDs or real jobs are not accidentally committed.
