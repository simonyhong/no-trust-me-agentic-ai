# No-Trust-Me Agentic AI — BRD Worker Prototype

> **Important change (October 2026):** This experimental version has **no automated test suite and no BRD-example or real-job approval tests**. Generated, cached and handcrafted functions are selected without executing test examples or checking expected job answers. The older project presentation in `docs/No_Trust_Me_Agentic_AI.pptx` describes a previous, test-gated architecture and is retained only as historical background. The project name is historical; it does not imply independently verified correctness.

## How it works

`job_manager.py` finds `documents/BRD_*.txt`, dispatches at most `max_concurrent_BRD_agents` workers, and handles lifecycle, scheduling, logging and watchdogs. `source/brd_processor.py` reads each BRD, loads an implementation and processes corresponding `BRD_*_jobs.json` records in arrival/file order. Implementations are tried in this order:

1. One optional handcrafted Python module from `saved_functions/<BRD stem>_handcrafted/`.
2. Previously saved generated implementation indexed by the current BRD hash in `saved_functions/registry.json`.
3. A newly GPT-generated Python implementation if no usable module is available.

**There is no pre-deployment behavior check.** Code is checked for parseability and disallowed constructs, and must expose the expected callable. During live jobs, Python still enforces input/output **types and structure** from the BRD contract, along with execution time limits. It does not compare answers with an `expected_*` field, run BRD examples, or sample labeled real jobs. A function returning the wrong but correctly typed answer can therefore be marked successful. Do not treat this version as quality-assured or safe for production.

## Repository layout

```text
job_manager.py                     Main entry point
source/brd_processor.py            Per-BRD worker and GPT code generator
documents/BRD_word_count.txt       Sanitized demo business requirement
documents/BRD_word_count_jobs.json Sanitized demo job inputs
docs/No_Trust_Me_Agentic_AI.pptx   Historical slides (outdated test-gate diagram)
saved_functions/                   Runtime function cache and registry
.env.example                       Private environment-variable template
requirements.txt                   Runtime dependencies
```

There is no `tests/` directory or automated CI test pipeline in this version.

## BRD authoring

Each `BRD_*.txt` must contain an underlined `FUNCTION INPUT/OUTPUT CONTRACT` heading with a JSON object containing ordered `input` definitions and an `output` definition. It also needs an underlined `JOBS DATA STRUCTURE` heading containing `ID_FIELD=<identifier>` on exactly one line. Other prose is free-form. Example:

```text
4. FUNCTION INPUT/OUTPUT CONTRACT
---------------------------------
{
  "input": [{"name": "text", "type": "string"}],
  "output": {"type": "integer"}
}

5. JOBS DATA STRUCTURE
----------------------
ID_FIELD=incident_id
```

Legacy `TEST EXAMPLES & EXPECTED RESULTS`, `N_ITEMS_TO_PASS` and `EXPECTED_FIELD` content in older BRDs is no longer parsed or used as an approval gate. If those sections remain, their prose may still be included in the BRD supplied to the code-generating LLM; they are **not executed by Python**. The included demo BRD omits them.

For `BRD_word_count.txt`, a jobs file `BRD_word_count_jobs.json` can contain `[{"incident_id":"A1","text":"Hello world"}]`. The generated function name is derived from the BRD filename, e.g. `word_count`.

## Running

Install dependencies, copy the template to `.env` and configure Azure OpenAI:

```bash
pip install -r requirements.txt
cp .env.example .env       # In PowerShell: Copy-Item .env.example .env
python job_manager.py
```

Configure `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT`, and optionally `AZURE_OPENAI_API_VERSION` in `.env`. Shell variables override `.env`. A handcrafted implementation can operate without Azure OpenAI when it is loadable and no GPT calls are needed.

### Completion and failure semantics

- Worker outputs are written to `all_process.log`. There is **no durable result payload file**, only job-status state.
- Each successful live job is recorded **immediately** in `documents/done_<BRD stem>_jobs.json`, via a temporary file and atomic replace, **before** the success is logged. Rejected malformed records are also recorded durably.
- If an existing done-state file is corrupt, unreadable, or cannot be written, processing raises `DoneStatePersistenceError` and the worker yields a `state_retry` status. JobManager retries the BRD after **45 seconds**, even if the jobs file is unchanged. A storage failure is not treated as a generated-function defect and cannot produce a successful batch result.
- If a side-effecting function performs an external action and the subsequent completion-state write fails, it may **perform that action again** on retry. Exactly-once processing requires a transactional destination or idempotent functions. Keep this version away from irreversible operations unless the operation is retry-safe.
- Live functions that raise exceptions or violate the output type/shape still trigger persistent human-review blocking and quarantine. A correct type/shape is not proof that a value is correct.
- `RESET_DONE_STATE=1` intentionally deletes completion state at startup for local debugging and can cause jobs to repeat.

### Scheduling and wake-ups

Each worker processes at most `MAX_JOBS_PER_WORKER_SESSION` actionable jobs (default 25) per session. The manager dispatches longest-waiting BRDs first, tracks jobs/done-file fingerprints and relaunches sleeping BRDs after file changes. A launch-time snapshot of the BRD/jobs files closes the lost wake-up gap; the done-file stamp is taken at worker exit. If input files change during a worker session, one additional relaunch may occur. File fingerprints are modification time plus size, not a durable change counter; upstream producers should write jobs JSON atomically.

### Human intervention

A BRD that exhausted generation attempts, failed at runtime, or crashed can remain persistently blocked by its BRD content hash. Changing the BRD resets that hash. For reviewed handwritten recovery, stop JobManager, place **one** `.py` in `saved_functions/<BRD stem>_handcrafted/`, then run:

```bash
python job_manager.py --approve-handcrafted BRD_word_count.txt
```

Restart the manager. Approval binds the current BRD hash to the exact handwritten source bytes. **This version unblocks on successful loading and byte-integrity verification, not passing behavioral tests.** Human inspection must therefore establish the implementation's correctness.

## Safety limitations

Generated source is screened by an AST allowlist and runs with the narrowly exposed `my_tools` wrappers (approved GPT call; approved Excel read under `documents/`). Nevertheless, it is executed using `exec()` in a worker process. Static restrictions and watchdogs **do not form a secure sandbox**; do not run untrusted generated code with sensitive credentials or host access. A killed worker holding a GPT semaphore slot may strand the permit until restart. `requirements.txt` is not version-pinned, so dependency resolution can also vary over time.

`.gitignore` intentionally excludes real BRD/job files, `.env`, runtime logs and generated state. Only sanitized example documents belong in this public repository.
