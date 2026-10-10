# No-Trust-Me Agentic AI — BRD Worker Prototype

## Motivation

This project was created to address a fundamental problem with LLM-generated code: **How can we trust code written by an LLM when the LLM itself can make mistakes?**

**No-Trust-Me Agentic AI** is a no-code agentic AI framework designed to reduce reliance on blindly trusting LLM-generated code.

Users describe their business requirements in a Business Requirement Document (BRD), and an LLM generates the necessary Python functions. However, instead of trusting the generated code—or asking the LLM to evaluate its own work—the system uses **independent Python-based validation** to check the function against user-defined test examples and real-job cases before deployment.

The key principle is simple: **The LLM writes the code, but Python—not the LLM—determines whether the code passes the required tests.**

Once validated, the function is saved and reused to process future jobs without repeatedly calling the code-generating LLM. Passing the prescribed tests reduces the risk of coding errors but does not guarantee correctness for every possible input.

The project has **two runtime Python modules**: `job_manager.py` and `source/brd_processor.py`. BRD/example validation and the blind real-job deployment gate live inside `brd_processor.py`, **not** in a separate developer `tests/` folder. That folder was intentionally removed.

## How it works

`job_manager.py` discovers `documents/BRD_*.txt`, starts bounded-concurrency workers, and manages scheduling, logging, cancellation and watchdogs. `source/brd_processor.py` parses each BRD, selects an implementation, independently validates it, then processes its jobs from the matching `BRD_*_jobs.json` file.

Implementation selection (for the current BRD content hash):

1. Optional handcrafted Python module in `saved_functions/<BRD stem>_handcrafted/` (exactly one `.py` file).
2. Previously saved generated implementation indexed by `saved_functions/registry.json`.
3. A new GPT-generated function when no acceptable existing implementation is available.

**New or modified implementations must pass Python-owned validation before first use:** all BRD-authored example tests, output contract checks, and the **first `N_ITEMS_TO_PASS` labeled real jobs** from the jobs file. A generated candidate that fails BRD examples can receive example-derived feedback and be regenerated up to ten times. If it passes examples but fails the blind N-job gate, the BRD is blocked for human review; the failing real-job inputs, labels and outputs are **not** sent back to the code-generating LLM. Passing these finite checks does not prove correctness for every possible input.

**A worker relaunch does not repeat deployment validation for unchanged, registered code.** The existing registry entry is keyed by BRD content hash. For generated code, registration stores its source hash; for handcrafted code, registration stores the filename and source hash. If those bytes and the BRD remain unchanged, the worker loads the function without BRD-example or N-job tests, even if the historical labeled jobs are no longer in the jobs file. An edited implementation must pass both gates again. Legacy generated registrations without a source hash are adopted once from the existing registered script (which was originally saved after successful validation), without demanding historical test jobs. No extra validation-status field or protocol version is added.

**Human-approved recovery of a blocked BRD always runs both gates again**, even if that handcrafted file was previously registered. Live labeled jobs are also checked against their expected answers; invalid output shapes or incorrect labeled answers trigger quarantine and persistent human-review blocking. An unlabeled live job can still execute, provided its inputs and outputs satisfy the declared contract.

## Repository layout

```text
job_manager.py                     Main manager entry point
source/brd_processor.py            BRD worker, code generator, validation and job runtime
source/__init__.py                 Python package marker
documents/BRD_word_count.txt       Sanitized BRD example, including Python-parsed test examples
documents/BRD_word_count_jobs.json Sanitized labeled jobs for the N-job gate
docs/No_Trust_Me_Agentic_AI.pptx   Project architecture presentation
saved_functions/                   Runtime implementation cache and registry
.env.example                       Environment configuration template
requirements.txt                   Python dependencies
```

There is no separate `tests/` directory or GitHub Actions regression-test workflow.

## BRD format

A BRD must contain these underlined sections. Their names are matched case-insensitively; Python parses the JSON and the standalone directives, rather than accepting interpretations from the code-generating LLM.

**FUNCTION INPUT/OUTPUT CONTRACT:** ordered input array and output contract:

```json
{"input": [{"name": "text", "type": "string"}], "output": {"type": "integer"}}
```

**TEST EXAMPLES & EXPECTED RESULTS:** one JSON array of user-authored examples:

```json
[{"input": "Hello world", "output": 2}, {"input": "", "output": 0}]
```

**JOBS DATA STRUCTURE:** describe the job records and provide exactly one line of each directive:

```text
ID_FIELD=incident_id
EXPECTED_FIELD=expected_word_count
N_ITEMS_TO_PASS=15
```

For this example the jobs JSON contains records such as:

```json
[{"incident_id": "A1", "text": "Hello world", "expected_word_count": 2}]
```

At least `N_ITEMS_TO_PASS` labeled jobs must be available for first-time qualification or requalification of modified code; they are **not** required for subsequent launches of an unchanged registered function. The demo BRD includes eight example tests; its jobs file includes fifteen labeled deployment jobs. The function name is derived from the filename: `BRD_word_count.txt` maps to `word_count()`.

## Run

Install dependencies and configure Azure OpenAI (a valid handcrafted implementation may run without calling GPT):

```bash
pip install -r requirements.txt
cp .env.example .env       # PowerShell: Copy-Item .env.example .env
python job_manager.py
```

Set `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT`, and optionally `AZURE_OPENAI_API_VERSION` in `.env`. Shell environment values take precedence. Keep secrets out of commits.

## Job completion, storage failures, and retries

- Job results are written to `all_process.log`. Completed IDs and rejected-record hashes are saved to `documents/done_<BRD stem>_jobs.json`; no separate durable result-payload file is produced.
- Each successful live job is saved **immediately**, using a temporary file and atomic replacement, **before** its success is logged. Rejected malformed jobs are also persisted before being logged.
- A corrupt/unreadable done-state file or failed state write causes `DoneStatePersistenceError` instead of silently resetting state or claiming success. The worker yields `state_retry` and the manager retries after **45 seconds**, even if the jobs file did not change. Storage errors do not quarantine the implementation.
- If a function performs an external side effect before the done-state write fails, the action can repeat on retry. Exactly-once side effects require idempotent operations or transactional destinations.
- `RESET_DONE_STATE=1` intentionally deletes local completion state at manager startup for debugging; it can cause reprocessing.

## Scheduling and oversight

Each worker processes at most `MAX_JOBS_PER_WORKER_SESSION` actionable jobs (25 by default) before yielding. The manager dispatches longest-waiting BRDs first and relaunches idle BRDs when BRD/jobs/done-file fingerprints change. BRD and jobs fingerprints are captured at launch to prevent missed wake-ups between the worker's final read and exit; done-file stamps are recorded after exit. An extra short relaunch can occur after changes during a worker's session, but an unchanged registered implementation does not repeat deployment validation. File metadata is an imperfect change detector; producers should update jobs JSON atomically.

Function execution and GPT tools have time/call budgets configured in `.env.example`. Failed deployed functions, exhausted generation attempts and unsuccessful blind N-job validation create persistent human-review blocks. The manager distinguishes ordinary worker yields from crashes and confirmed watchdog violations. An approved handwritten recovery requires a stopped manager, exactly one reviewed `.py` in the corresponding handcrafted directory, and:

```bash
python job_manager.py --approve-handcrafted BRD_word_count.txt
```

Restart the manager afterward. Approval binds the BRD hash to the exact handcrafted source bytes, and the replacement must **still pass both BRD examples and the N labeled real jobs** before the persistent block is lifted. There is no automatic GPT fallback in that recovery attempt.

## Security limits

Generated source is statically screened, and external capabilities are exposed through restricted `my_tools` wrappers. Nevertheless, it still executes with `exec()` in the worker process. Static checks and watchdogs are **not** a security sandbox; use a controlled environment and do not treat the finite validation gate as an adversarial security proof. A force-killed worker may strand a shared GPT semaphore permit. This is a prototype, not a hardened production executor.

`.gitignore` excludes private real BRDs/jobs, generated state, logs, and `.env`. Only the sanitized demonstration is tracked in the public repository.
