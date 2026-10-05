import json
import pathlib
import textwrap
import time
import os
import re
from datetime import datetime, timezone
import hashlib
import importlib.util
import random
import multiprocessing
import queue
import openai
from openai import APIStatusError, RateLimitError
from typing import Callable, Any, List, Set, Tuple, Dict, Optional
import traceback
import tempfile
import logging, sys

_RETRY_STEPS = 6
MAX_ATTEMPTS = 10
LOG = logging.getLogger("brd_processor")

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")

gpt_client = None
if AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT:
    gpt_client = openai.AzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )

############ Make a Synthetic Module to import  #####
# This "Synthetic Module" is needed because some objects, like gpt_semaphore, (which is handed over by job_manager.py) are not initially global, so I register: "my_tools.gpt_semaphore = gpt_semaphore"
# and I do not want GPT to include these objects as part of the arguements in the function that it makes.
import types
toolbox = types.ModuleType("my_tools")   # synthetic; lives only in memory
toolbox.log     = logging.getLogger("my_tools") 
toolbox.gpt_client = gpt_client                    
toolbox.gpt_model = AZURE_OPENAI_DEPLOYMENT 
sys.modules["my_tools"] = toolbox    # register the module
import my_tools                      # fetch the same module object
######################################################

def _atomic_write_json(target: pathlib.Path, data: dict) -> None:
    """
    Atomically write *data* to *target* as pretty-printed JSON.

    1.  Write to a file created in the same directory (guarantees the
        final rename is on the same filesystem).
    2.  `os.fsync()` to flush to disk.
    3.  `os.replace()` - atomic on POSIX & Windows - to swap it in.
    """
    tmp_fd, tmp_name = tempfile.mkstemp(dir=target.parent)
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, target)          # atomic
    except Exception:
        # ensure no orphaned temp files hang around
        try:
            os.unlink(tmp_name)
        finally:
            raise

def load_script(*, namespace: str, py_path: pathlib.Path | None = None, 
                saved_func_dir: pathlib.Path | None = None,
                hash_signature: str | None = None, 
                script_name: str | None = None,
                registry_path: pathlib.Path | None = None):
    """
    Load a Python file as a module under the given `namespace`.

    Use EITHER:
      - `py_path` (direct path to a .py file), OR
      - (`saved_func_dir`, `hash_signature`, `script_name`) to resolve the file.

    Returns
    -------
    module
    """
    if py_path is None:
        if not (saved_func_dir and hash_signature and script_name):
            raise ValueError("Provide either `py_path` OR (saved_func_dir, hash_signature, script_name).")
        
        # Load registry to get the folder name
        if registry_path is None:
            # Default registry path - adjust this to match your actual registry location
            registry_path = saved_func_dir / "registry.json"
        
        registry = load_registry(registry_path)
        folder_name = registry.get(hash_signature, {}).get("folder_name", hash_signature)
        
        py_path = saved_func_dir / folder_name / script_name

    if not py_path.exists():
        raise FileNotFoundError(py_path)

    spec = importlib.util.spec_from_file_location(namespace, py_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {py_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod

def gpt_extract_function_and_data_schema(brd_text: str, gpt_semaphore: multiprocessing.Semaphore) -> dict[str, Any]:
    """Extract only function name and optional runtime-job metadata.

    Function input names/types and the output contract are NOT inferred by the LLM;
    Python reads those from the BRD's FUNCTION INPUT/OUTPUT CONTRACT JSON section.
    """
    SYSTEM = (
        "You are a technical requirements analyst.\n"
        "Read the Business Requirement Document (BRD).\n"
        "Python separately parses the BRD's FUNCTION INPUT/OUTPUT CONTRACT and TEST EXAMPLES sections.\n"
        "Do NOT use generic test keys such as 'input' or 'output' as job-field names.\n"
        "From the BRD, choose the Python function name and identify runtime-job metadata from the JOBS DATA STRUCTURE section.\n"
        "Return exactly ONE line of valid JSON.\n\n"
        "Required keys\n"
        "-------------\n"
        '  "function_name" : snake_case Python identifier (string)\n'
        '  "incident_id"   : runtime job field used to uniquely identify a job record (string)\n\n'
        "Required when labeled jobs are defined in JOBS DATA STRUCTURE\n"
        "-----------------------------------------------------------\n"
        '  "expected" : runtime/development job field containing the target/expected output\n\n'
        "Important\n"
        "---------\n"
        "• N_ITEMS_TO_PASS is BRD deployment metadata, NOT a job-record field.\n"
        "• If N_ITEMS_TO_PASS is present, identify the job-record field that contains the target answer and return it as 'expected'.\n\n"
        "Formatting rules\n"
        "---------------\n"
        "• One line of strict JSON; no markdown, prose, or comments.\n"
        "• Use only the keys above.\n"
    )

    resp = gpt_call_with_retry(
        gpt_semaphore,
        messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": brd_text}],
        temperature=0,
    )
    txt = resp.choices[0].message.content.strip()
    try:
        schema = json.loads(txt)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GPT job-metadata JSON invalid: {exc}\n---\n{txt}") from exc
    if not isinstance(schema, dict) or not schema.get("function_name"):
        raise ValueError(f"GPT job-metadata JSON missing required function_name: {schema!r}")
    if not schema.get("incident_id"):
        raise ValueError(f"GPT job-metadata JSON missing required incident_id: {schema!r}")
    return schema


_ID_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")   # valid Python identifier
def brd_to_function_name(brd_path: pathlib.Path) -> str:
    """
    Return the BRD file's stem verbatim (e.g.  BRD_email_router → same).
    Abort early if the stem cannot be used as a Python identifier.
    """
    stem = brd_path.stem            # e.g. BRD_email_router
    if not stem.startswith("BRD_"):
        raise ValueError(f"{brd_path.name!s}: expected file name to start with 'BRD_'")

    if not _ID_RE.fullmatch(stem):
        raise ValueError(
            f"{stem!r} contains characters that are illegal in a Python identifier.\n"
            "Either rename the BRD file or switch to a snake-case conversion."
        )
    return stem                    
    
def ask_gpt_with_naming_convention_to_make_func(
    gpt_semaphore: multiprocessing.Semaphore,
    brd_text: str,
    expected_function_name: str,
    param_names: list[str],
    reflection: str | None = None,
) -> str:
    """Generate implementation code only. BRD contract/tests remain authoritative."""
    sig_params = ", ".join(param_names) if param_names else "text"
    function_template = (
        f"def {expected_function_name}({sig_params}):\n"
        f"    # Your implementation here\n"
        f"    return result"
    )

    system_prompt = f"""
You are a senior Python engineer implementing a business requirement.

CRITICAL: The function **must** be named **{expected_function_name}**.

Use this exact signature **verbatim**:
{function_template}

The BRD contains two inspector-facing sections that are authoritative and machine-readable:
1. FUNCTION INPUT/OUTPUT CONTRACT — one JSON object with an "input" array defining ordered inputs and an "output" object defining the output schema.
2. TEST EXAMPLES & EXPECTED RESULTS — one JSON array containing only user-authored tests.
Python parses and enforces both sections directly.
Follow the FUNCTION INPUT/OUTPUT CONTRACT exactly, including return type, nested structure,
length constraints, and allowed values.
Do NOT invent, modify, reinterpret, add, or remove tests, expected outputs, contract fields,
types, or output structure. Your job is only to implement the function.

Return executable Python source code ONLY.
Do not add prose before or after the code.
"""

    user_prompt = ""
    if reflection:
        user_prompt += reflection + "\n\n"
    user_prompt += "Requirement:\n" + brd_text

    resp = gpt_call_with_retry(
        gpt_semaphore,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0 if not reflection else 0.4,
    )
    return resp.choices[0].message.content.strip()


def clean_generated_code(resp_text: str) -> str:
    """Accept code only; strip one surrounding markdown fence if that is the whole response."""
    code = textwrap.dedent(resp_text).strip()
    if not code:
        raise ValueError("GPT returned empty source code")
    fenced = re.fullmatch(r"```[A-Za-z0-9_+-]*\n(.*?)\n?```", code, flags=re.DOTALL)
    if fenced:
        code = textwrap.dedent(fenced.group(1)).strip()
    if not code:
        raise ValueError("GPT returned empty source code")
    if "```" in code:
        raise ValueError("GPT returned markdown/prose around executable code")
    return code


def _extract_section_body(brd_text: str, heading: str) -> str:
    """Return text belonging to a numbered BRD section identified by heading text."""
    lines = brd_text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if heading.upper() in line.upper():
            start = i + 1
            break
    if start is None:
        raise ValueError(f"BRD is missing required section: {heading}")

    end = len(lines)
    for i in range(start, len(lines)):
        stripped = lines[i].strip()
        if i > start and (
            re.match(r"^\d+\.\s+\S", stripped)
            or "TECHNICAL DESCRIPTIONS" in stripped.upper()
        ):
            end = i
            break
    return "\n".join(lines[start:end])


def _decode_first_json_value(section_body: str, opening: str, section_name: str):
    """Decode the first JSON value beginning with *opening* inside a machine-readable section."""
    pos = section_body.find(opening)
    if pos < 0:
        raise ValueError(f"{section_name} does not contain a JSON value beginning with {opening!r}")
    try:
        value, _ = json.JSONDecoder().raw_decode(section_body[pos:])
    except json.JSONDecodeError as exc:
        raise ValueError(f"{section_name} contains invalid JSON: {exc}") from exc
    return value


def _validate_contract_spec_definition(spec: dict, path: str) -> None:
    if not isinstance(spec, dict):
        raise ValueError(f"{path} must be a JSON object")
    type_name = spec.get("type")
    valid_types = {"string", "integer", "number", "boolean", "array", "object", "null"}
    if type_name not in valid_types:
        raise ValueError(f"{path}.type must be one of {sorted(valid_types)}; got {type_name!r}")

    if "allowed_values" in spec and not isinstance(spec["allowed_values"], list):
        raise ValueError(f"{path}.allowed_values must be a JSON array")

    if type_name == "array":
        if "length" in spec and (type(spec["length"]) is not int or spec["length"] < 0):
            raise ValueError(f"{path}.length must be a non-negative integer")
        items = spec.get("items")
        if items is not None:
            if isinstance(items, list):
                for i, child in enumerate(items):
                    _validate_contract_spec_definition(child, f"{path}.items[{i}]")
            elif isinstance(items, dict):
                _validate_contract_spec_definition(items, f"{path}.items")
            else:
                raise ValueError(f"{path}.items must be an object or array of objects")

    if type_name == "object":
        properties = spec.get("properties")
        if properties is not None:
            if not isinstance(properties, dict):
                raise ValueError(f"{path}.properties must be a JSON object")
            for key, child in properties.items():
                _validate_contract_spec_definition(child, f"{path}.properties[{key!r}]")
        required = spec.get("required")
        if required is not None and (not isinstance(required, list) or not all(isinstance(k, str) for k in required)):
            raise ValueError(f"{path}.required must be an array of strings")
        if "additional_properties" in spec and type(spec["additional_properties"]) is not bool:
            raise ValueError(f"{path}.additional_properties must be boolean")


def extract_brd_contract(brd_text: str) -> dict[str, Any]:
    """Parse and validate the BRD author's JSON FUNCTION INPUT/OUTPUT CONTRACT."""
    body = _extract_section_body(brd_text, "FUNCTION INPUT/OUTPUT CONTRACT")
    contract = _decode_first_json_value(body, "{", "FUNCTION INPUT/OUTPUT CONTRACT")
    if not isinstance(contract, dict):
        raise ValueError("FUNCTION INPUT/OUTPUT CONTRACT must contain one JSON object")

    inputs = contract.get("input")
    output = contract.get("output")
    if not isinstance(inputs, list) or not inputs:
        raise ValueError("FUNCTION INPUT/OUTPUT CONTRACT.input must be a non-empty JSON array")
    if not isinstance(output, dict):
        raise ValueError("FUNCTION INPUT/OUTPUT CONTRACT.output must be a JSON object")

    names: list[str] = []
    for i, input_spec in enumerate(inputs):
        if not isinstance(input_spec, dict):
            raise ValueError(f"contract.input[{i}] must be a JSON object")
        name = input_spec.get("name")
        if not isinstance(name, str) or not _ID_RE.fullmatch(name):
            raise ValueError(f"contract.input[{i}].name must be a valid Python identifier")
        if name in names:
            raise ValueError(f"duplicate input name in contract: {name!r}")
        names.append(name)
        _validate_contract_spec_definition(input_spec, f"contract.input[{i}]")

    _validate_contract_spec_definition(output, "contract.output")
    return contract


def _strict_equal(actual: Any, expected: Any) -> bool:
    """Compare value, Python type, and nested structure strictly."""
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, (list, tuple)):
        return len(actual) == len(expected) and all(_strict_equal(a, e) for a, e in zip(actual, expected))
    if isinstance(expected, dict):
        if len(actual) != len(expected) or set(actual.keys()) != set(expected.keys()):
            return False
        return all(_strict_equal(actual[k], expected[k]) for k in expected)
    return actual == expected


def _validate_value_against_contract(value: Any, spec: dict, path: str) -> tuple[bool, str | None]:
    """Validate a Python value against a JSON-defined BRD type/shape contract."""
    type_name = spec["type"]
    type_ok = {
        "string": type(value) is str,
        "integer": type(value) is int,
        "number": type(value) in (int, float),
        "boolean": type(value) is bool,
        "array": type(value) is list,
        "object": type(value) is dict,
        "null": value is None,
    }[type_name]
    if not type_ok:
        return False, f"{path} must have type {type_name}; got {type(value).__name__}"

    if "allowed_values" in spec and not any(_strict_equal(value, allowed) for allowed in spec["allowed_values"]):
        return False, f"{path} must be one of {spec['allowed_values']!r}; got {value!r}"

    if type_name == "array":
        if "length" in spec and len(value) != spec["length"]:
            return False, f"{path} must contain exactly {spec['length']} items; got {len(value)}"
        items = spec.get("items")
        if isinstance(items, list):
            if len(value) != len(items):
                return False, f"{path} must contain {len(items)} positionally specified items; got {len(value)}"
            for i, (item, child_spec) in enumerate(zip(value, items)):
                ok, reason = _validate_value_against_contract(item, child_spec, f"{path}[{i}]")
                if not ok:
                    return False, reason
        elif isinstance(items, dict):
            for i, item in enumerate(value):
                ok, reason = _validate_value_against_contract(item, items, f"{path}[{i}]")
                if not ok:
                    return False, reason

    if type_name == "object":
        properties = spec.get("properties") or {}
        required = spec.get("required")
        if required is None and properties:
            required = list(properties.keys())
        for key in required or []:
            if key not in value:
                return False, f"{path} is missing required key {key!r}"
        if spec.get("additional_properties") is False:
            extra = [key for key in value if key not in properties]
            if extra:
                return False, f"{path} contains unexpected keys {extra!r}"
        for key, child_spec in properties.items():
            if key in value:
                ok, reason = _validate_value_against_contract(value[key], child_spec, f"{path}.{key}")
                if not ok:
                    return False, reason

    return True, None


def extract_brd_tests(brd_text: str, contract: dict[str, Any]) -> list[tuple[tuple[Any, ...], Any, int]]:
    """Parse only the user-authored JSON tests embedded in the BRD."""
    body = _extract_section_body(brd_text, "TEST EXAMPLES & EXPECTED RESULTS")
    raw_tests = _decode_first_json_value(body, "[", "TEST EXAMPLES & EXPECTED RESULTS")
    if not isinstance(raw_tests, list) or not raw_tests:
        raise ValueError("TEST EXAMPLES & EXPECTED RESULTS must contain a non-empty JSON array")

    input_specs = contract["input"]
    param_names = [spec["name"] for spec in input_specs]
    output_spec = contract["output"]
    tests: list[tuple[tuple[Any, ...], Any, int]] = []

    for test_index, test in enumerate(raw_tests, start=1):
        if not isinstance(test, dict) or "input" not in test or "output" not in test:
            raise ValueError(f"BRD test #{test_index} must be an object containing 'input' and 'output'")
        raw_input = test["input"]
        expected = test["output"]

        if len(input_specs) == 1:
            args = (raw_input,)
            ok, reason = _validate_value_against_contract(raw_input, input_specs[0], f"test #{test_index}.input")
            if not ok:
                raise ValueError(reason)
        else:
            if not isinstance(raw_input, dict):
                raise ValueError(
                    f"BRD test #{test_index}.input must be an object keyed by {param_names!r} for a multi-input function"
                )
            missing = [name for name in param_names if name not in raw_input]
            extra = [name for name in raw_input if name not in param_names]
            if missing or extra:
                raise ValueError(f"BRD test #{test_index}.input keys mismatch; missing={missing}, extra={extra}")
            args_list = []
            for input_spec in input_specs:
                name = input_spec["name"]
                value = raw_input[name]
                ok, reason = _validate_value_against_contract(value, input_spec, f"test #{test_index}.input.{name}")
                if not ok:
                    raise ValueError(reason)
                args_list.append(value)
            args = tuple(args_list)

        ok, reason = _validate_value_against_contract(expected, output_spec, f"test #{test_index}.output")
        if not ok:
            raise ValueError(reason)
        tests.append((args, expected, test_index))

    return tests



_N_ITEMS_TO_PASS_RE = re.compile(r"^N_ITEMS_TO_PASS=([1-9][0-9]*)$")


def extract_n_items_to_pass(brd_text: str) -> int:
    """Read the rigid deployment-validation count from JOBS DATA STRUCTURE.

    The BRD must contain exactly one standalone line in that section:
        N_ITEMS_TO_PASS=<positive integer>

    Example:
        N_ITEMS_TO_PASS=5
    """
    body = _extract_section_body(brd_text, "JOBS DATA STRUCTURE")
    values: list[int] = []
    malformed: list[str] = []

    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("N_ITEMS_TO_PASS"):
            match = _N_ITEMS_TO_PASS_RE.fullmatch(line)
            if match is None:
                malformed.append(line)
            else:
                values.append(int(match.group(1)))

    if malformed:
        raise ValueError(
            "JOBS DATA STRUCTURE contains malformed N_ITEMS_TO_PASS line(s): "
            f"{malformed!r}. Use exactly N_ITEMS_TO_PASS=<positive integer>, e.g. N_ITEMS_TO_PASS=5"
        )
    if not values:
        raise ValueError(
            "JOBS DATA STRUCTURE must contain exactly one standalone line "
            "N_ITEMS_TO_PASS=<positive integer>, e.g. N_ITEMS_TO_PASS=5"
        )
    if len(values) != 1:
        raise ValueError(
            f"JOBS DATA STRUCTURE must contain exactly one N_ITEMS_TO_PASS line; found {len(values)}"
        )
    return values[0]


def _load_jobs_list(jobs_path: pathlib.Path) -> list[dict[str, Any]]:
    """Load a jobs file as a list of JSON objects for deterministic inspection."""
    if not jobs_path.exists():
        raise FileNotFoundError(f"Required jobs file not found: {jobs_path.name}")
    try:
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Failed to read/parse jobs file {jobs_path.name}: {exc}") from exc
    if not isinstance(jobs, list):
        raise ValueError(f"Jobs file {jobs_path.name} must contain a JSON list of records")
    for i, rec in enumerate(jobs, start=1):
        if not isinstance(rec, dict):
            raise ValueError(f"Jobs file {jobs_path.name} item #{i} must be a JSON object")
    return jobs


def prepare_deployment_validation_jobs(
    jobs_path: pathlib.Path,
    jobs_schema: dict[str, Any],
    contract: dict[str, Any],
    n_items_to_pass: int,
) -> tuple[list[dict[str, Any]], int]:
    """Return the first N labeled jobs, after validating their data contract.

    A labeled job is a record containing the BRD-specific expected-output field
    identified from JOBS DATA STRUCTURE. Unlabeled records do not count toward N.
    """
    jobs = _load_jobs_list(jobs_path)
    exp_field = jobs_schema.get("expected")
    id_field = jobs_schema.get("incident_id")
    param_names = list(jobs_schema.get("inputs") or [])

    if not exp_field:
        raise ValueError(
            "Could not identify the expected-output field from JOBS DATA STRUCTURE; "
            "N_ITEMS_TO_PASS requires labeled jobs"
        )
    if not id_field:
        raise ValueError("Could not identify incident_id field from JOBS DATA STRUCTURE")

    labeled = [rec for rec in jobs if exp_field in rec]
    if len(labeled) < n_items_to_pass:
        raise ValueError(
            f"{jobs_path.name} has only {len(labeled)} labeled job(s) using expected field "
            f"{exp_field!r}, but N_ITEMS_TO_PASS={n_items_to_pass}"
        )

    selected = labeled[:n_items_to_pass]
    input_specs_by_name = {spec["name"]: spec for spec in contract["input"]}
    output_spec = contract["output"]

    seen_ids: set[str] = set()
    for ordinal, rec in enumerate(selected, start=1):
        if id_field not in rec:
            raise ValueError(f"Deployment-validation job #{ordinal} is missing {id_field!r}")
        incident_id = str(rec[id_field])
        if incident_id in seen_ids:
            raise ValueError(f"Duplicate incident id among first {n_items_to_pass} labeled jobs: {incident_id!r}")
        seen_ids.add(incident_id)

        for name in param_names:
            if name not in rec:
                raise ValueError(
                    f"Deployment-validation job {incident_id!r} is missing required input field {name!r}"
                )
            spec = input_specs_by_name[name]
            ok, reason = _validate_value_against_contract(
                rec[name], spec, f"deployment job {incident_id}.{name}"
            )
            if not ok:
                raise ValueError(reason)

        ok, reason = _validate_value_against_contract(
            rec[exp_field], output_spec, f"deployment job {incident_id}.{exp_field}"
        )
        if not ok:
            raise ValueError(reason)

    return selected, len(labeled)


def run_deployment_validation_jobs(
    func: Callable[..., Any],
    jobs_schema: dict[str, Any],
    deployment_jobs: list[dict[str, Any]],
    output_contract: dict[str, Any],
) -> tuple[bool, Optional[str]]:
    """Require the candidate function to pass the preselected labeled jobs exactly."""
    param_names = list(jobs_schema.get("inputs") or [])
    id_field = jobs_schema.get("incident_id")
    exp_field = jobs_schema.get("expected")

    for ordinal, rec in enumerate(deployment_jobs, start=1):
        incident_id = str(rec.get(id_field, "?")) if id_field else "?"
        params = [rec[name] for name in param_names]
        expected = rec[exp_field]

        try:
            result = func(*params)
        except Exception as exc:
            reflection = (
                f"Function crashed on deployment-validation job #{ordinal} "
                f"(incident_id={incident_id}).\n\n"
                f"Inputs passed: {params}\n"
                f"Exception: {type(exc).__name__}: {exc}\n"
                f"Traceback (top 2 frames):\n{traceback.format_exc(limit=2)}"
            )
            LOG.warning("❌ Deployment job #%d incident_id=%s crashed: %s", ordinal, incident_id, exc)
            return False, reflection

        valid, reason = _validate_value_against_contract(
            result, output_contract, f"deployment job #{ordinal} output"
        )
        if not valid:
            reflection = (
                f"Function violated the BRD output contract on deployment-validation job #{ordinal} "
                f"(incident_id={incident_id}).\n\n"
                f"Inputs passed: {params}\n"
                f"Got: {result!r} (type {type(result).__name__})\n"
                f"Contract violation: {reason}"
            )
            LOG.warning("❌ Deployment job #%d incident_id=%s contract violation: %s", ordinal, incident_id, reason)
            return False, reflection

        if not _strict_equal(result, expected):
            reflection = (
                f"Function returned the wrong result on deployment-validation job #{ordinal} "
                f"(incident_id={incident_id}).\n\n"
                f"Inputs passed: {params}\n"
                f"Expected: {expected!r} (type {type(expected).__name__})\n"
                f"Got: {result!r} (type {type(result).__name__})"
            )
            LOG.warning(
                "❌ Deployment job #%d incident_id=%s: got %r, expected %r",
                ordinal, incident_id, result, expected,
            )
            return False, reflection

        LOG.info(
            "✅ Deployment job #%d incident_id=%s -> OK (%r)",
            ordinal, incident_id, result,
        )

    return True, None


def run_brd_tests(
    func: Callable[..., Any],
    tests: list[tuple[tuple[Any, ...], Any, int]],
    contract: dict[str, Any],
) -> tuple[bool, list[str]]:
    """Execute only BRD-authored tests; Python enforces contract and expected values."""
    feedback: list[str] = []
    ok = True
    output_spec = contract["output"]

    for args, expected, test_index in tests:
        try:
            got = func(*args)
        except Exception as exc:
            feedback.append(f"BRD test #{test_index}: args={args!r} -> raised {type(exc).__name__}: {exc}")
            ok = False
            continue

        valid, reason = _validate_value_against_contract(got, output_spec, f"BRD test #{test_index} output")
        if not valid:
            feedback.append(f"BRD test #{test_index}: args={args!r} -> output contract violation: {reason}; got {got!r}")
            ok = False
            continue

        if not _strict_equal(got, expected):
            feedback.append(
                f"BRD test #{test_index}: args={args!r} -> expected {expected!r} "
                f"(type {type(expected).__name__}), got {got!r} (type {type(got).__name__})"
            )
            ok = False
        else:
            feedback.append(f"BRD test #{test_index}: args={args!r} -> OK ({got!r})")

    for line in feedback:
        if " -> OK " in line:
            LOG.info("✅ %s", line)
        else:
            LOG.warning("❌ %s", line)
    return ok, feedback


def save_registry(reg_path: pathlib.Path, reg: dict) -> None:
    """Atomically replace the registry file. Read-modify-write callers must hold the shared registry lock."""
    _atomic_write_json(reg_path, reg)

def load_registry(reg_path: pathlib.Path) -> dict:
    """
    Read the registry without explicit locks.
    Because `save_registry` always swaps the fully-written file in atomically, a reader will either see the *old* complete file or the
    *new* complete file - never a half-written one.  If the file happens to be missing or corrupted we just return an empty dict.
    """
    try:
        with open(reg_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_source(hash_signature: str, code: str, func_name: str, brd_path: pathlib.Path,
                saved_func_dir, reg_path, registry_lock=None) -> str:
    brd_stem = brd_path.stem
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{brd_stem}_{ts}.py"
    sig_dir = saved_func_dir / brd_stem
    path = sig_dir / filename

    def _save_locked():
        sig_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(code, encoding="utf-8")

        registry = load_registry(reg_path)
        existing = registry.get(hash_signature, {})
        quarantine = existing.get("quarantine", [])
        existing.update({
            "title": brd_path.name,
            "latest_script": filename,
            "function_name": func_name,
            "folder_name": brd_stem,
        })
        existing.pop("last_examples", None)
        if quarantine:
            existing["quarantine"] = quarantine
        registry[hash_signature] = existing
        save_registry(reg_path, registry)

    if registry_lock is None:
        _save_locked()
    else:
        with registry_lock:
            _save_locked()

    LOG.info("Saved new implementation to %s", path)
    return filename


def brd_hash_signature(txt: str, length: int = 8) -> str:
    """8-hex BLAKE2 digest that identifies the BRD contents."""
    return hashlib.blake2s(txt.encode(), digest_size=16).hexdigest()[:length]


def gpt_call_with_retry(semaphore, **kwargs):
    if gpt_client is None or not my_tools.gpt_model:
        raise RuntimeError(
            "Azure OpenAI is not configured. Set AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, "
            "and AZURE_OPENAI_DEPLOYMENT. AZURE_OPENAI_API_VERSION is optional."
        )
    with semaphore:                               # global quota guard
        backoff = 1.0
        for _ in range(_RETRY_STEPS):
            try:
                resp = gpt_client.chat.completions.create(model=my_tools.gpt_model, **kwargs)
                return resp
            except (RateLimitError, APIStatusError) as exc:
                if getattr(exc, "status_code", None) != 429:
                    LOG.exception("GPT hard error")
                    raise
            sleep = backoff + random.uniform(0, 0.3)
            LOG.warning("GPT 429 - retry in %.1fs", sleep)
            time.sleep(sleep)
            backoff = min(backoff * 2, 60)
        raise RuntimeError("GPT call failed after retries")


def _read_done_ids(done_file: pathlib.Path) -> set[str]:
    try:
        data = json.loads(done_file.read_text(encoding="utf-8"))
        return set(data["ids"])
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return set()


def process_jobs(
    func: Callable[..., Any],
    jobs_schema: Dict[str, Any],
    jobs_path: pathlib.Path,
    done_file: Optional[pathlib.Path] = None,
    *,
    output_contract: dict[str, Any] | None = None,
    preview_limit: int = 1200,
    compare_expected: bool = True,
) -> Tuple[bool, Optional[str]]:
    """Run supplied jobs and enforce the BRD output contract.

    compare_expected=True is available for explicit evaluation workflows. Normal
    post-deployment processing uses False because correctness was already gated
    by the first N labeled deployment-validation jobs.
    """
    if not jobs_path.exists():
        LOG.info("No jobs file found for %s - skipping batch run.", jobs_path.name)
        return True, None

    try:
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOG.error("Failed to read or parse jobs file %s: %s", jobs_path.name, exc, exc_info=True)
        raise

    if not isinstance(jobs, list):
        raise ValueError(f"Jobs file {jobs_path.name} must contain a JSON list of records")
    if not jobs:
        LOG.info("Empty jobs file %s - nothing to do.", jobs_path.name)
        return True, None

    param_names: List[str] = list(jobs_schema.get("inputs") or [])
    id_field: Optional[str] = jobs_schema.get("incident_id")
    exp_field: Optional[str] = jobs_schema.get("expected")

    if id_field:
        if done_file is None:
            done_file = jobs_path.parent / f"done_{jobs_path.stem}.json"
        done_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            raw = json.loads(done_file.read_text(encoding="utf-8"))
            done_ids: Set[str] = set(raw.get("ids", []))
        except Exception:
            done_ids = set()
    else:
        done_ids = set()
        done_file = None

    new_jobs = [rec for rec in jobs if str(rec.get(id_field)) not in done_ids] if id_field else jobs
    if not new_jobs:
        LOG.info("All %d jobs in %s already processed - nothing to do.", len(jobs), jobs_path.name)
        return True, None

    LOG.info("Batch start for %s: %d new / %d total", jobs_path.name, len(new_jobs), len(jobs))

    for rec in new_jobs:
        incident_id = str(rec.get(id_field)) if id_field else "?"
        params = [rec.get(k, "") for k in param_names]
        try:
            result = func(*params)

            if output_contract is not None:
                valid, reason = _validate_value_against_contract(result, output_contract, "function output")
                if not valid:
                    reflection = (
                        "Function violated the BRD output contract on a supplied job.\n\n"
                        f"Inputs passed: {params}\n"
                        f"Got: {result!r} (type {type(result).__name__})\n"
                        f"Contract violation: {reason}"
                    )
                    LOG.warning("incident_id=%s: output contract violation: %s", incident_id, reason)
                    return False, reflection

            if compare_expected and exp_field and exp_field in rec:
                expected = rec[exp_field]
                if not _strict_equal(result, expected):
                    hints: List[str] = []
                    if isinstance(result, tuple) and isinstance(expected, list):
                        hints.append("Return a list ([]) not a tuple (()).")
                    if isinstance(result, (list, tuple)) and isinstance(expected, (list, tuple)) and len(result) != len(expected):
                        hints.append(f"Sequence length differs: got {len(result)} elements, expected {len(expected)}.")
                    if isinstance(result, dict) and isinstance(expected, dict):
                        missing = [k for k in expected if k not in result]
                        extra = [k for k in result if k not in expected]
                        if missing:
                            hints.append(f"Missing keys: {missing}")
                        if extra:
                            hints.append(f"Unexpected keys: {extra}")

                    try:
                        record_json = json.dumps(rec, ensure_ascii=False)
                    except Exception:
                        record_json = "<unserializable record>"
                    if len(record_json) > preview_limit:
                        record_json = record_json[:preview_limit] + " …<truncated>"

                    reflection = (
                        "Function returned a wrong result on a supplied development job.\n\n"
                        f"Inputs passed: {params}\n"
                        f"Expected: {expected!r} (type {type(expected).__name__})\n"
                        f"Got: {result!r} (type {type(result).__name__})\n"
                        f"Job record (trimmed JSON): {record_json}"
                    )
                    if hints:
                        reflection += "\nHints: " + " ".join(hints)
                    LOG.warning(
                        "incident_id=%s: got %r (type %s), expected %r (type %s)",
                        incident_id, result, type(result).__name__, expected, type(expected).__name__,
                    )
                    return False, reflection

            LOG.info("incident_id=%s: %s", incident_id, result)
            if id_field and incident_id != "?":
                done_ids.add(incident_id)

        except Exception as exc:
            try:
                record_json = json.dumps(rec, ensure_ascii=False)
            except Exception:
                record_json = "<unserializable record>"
            if len(record_json) > preview_limit:
                record_json = record_json[:preview_limit] + " …<truncated>"
            reflection = (
                "Function crashed on a supplied job.\n\n"
                f"Exception: {type(exc).__name__}: {exc}\n"
                f"Traceback (top 2 frames):\n{traceback.format_exc(limit=2)}\n\n"
                f"Inputs passed: {params}\n"
                f"Job record (trimmed JSON): {record_json}"
            )
            LOG.error("incident_id=%s crashed: %s", incident_id, exc, exc_info=True)
            return False, reflection

    if id_field and done_file is not None:
        try:
            _atomic_write_json(done_file, {"ids": sorted(done_ids)})
        except Exception as exc:
            LOG.warning("Could not write done file %s: %s", done_file, exc)
    return True, None



HB_MAX_AGE = 9.0
HB_MISSES_BEFORE_EXIT = 3


def _check_manager_heartbeat_shared(shared_data, max_age: float = 15.0):
    """
    Returns (ok, reason). ok=True if a recent heartbeat exists in shared_data.
    Robust against Manager going away (raises) or missing fields.
    """
    try:
        sd = shared_data.get("shared_dict") if hasattr(shared_data, "get") else shared_data["shared_dict"]
        hb = sd.get("hb")
        if not hb:
            return False, "no_hb_key"
        ts = float(hb.get("ts", 0))
        if ts <= 0:
            return False, "no_ts"
        age = time.time() - ts
        if age <= max_age:
            return True, f"fresh(age={age:.1f}s)"
        return False, f"stale(age={age:.1f}s)"
    except (BrokenPipeError, EOFError, OSError) as exc:
        # Manager server likely dead; treat as no heartbeat
        return False, f"proxy_error:{type(exc).__name__}"
    except Exception as exc:
        return False, f"error:{type(exc).__name__}"
        
def _get_or_create_function(
    brd_path: pathlib.Path,
    brd_text: str,
    hash_signature: str,
    schema: dict,
    contract: dict[str, Any],
    brd_tests: list[tuple[tuple[Any, ...], Any, int]],
    n_items_to_pass: int,
    deployment_jobs: list[dict[str, Any]],
    saved_func_dir: pathlib.Path,
    registry_path: pathlib.Path,
    gpt_semaphore: multiprocessing.Semaphore,
    registry_lock,
    logger,
    is_debug_mode: bool,
    reflection: str | None = None,
):
    """Load or generate an implementation and let Python enforce the BRD contract/tests."""
    skip_reuse_once = bool(reflection)
    expected_function_name = schema.get("function_name") or brd_to_function_name(brd_path)
    param_names = [input_spec["name"] for input_spec in contract["input"]]

    jobs_schema = {
        "inputs": param_names,
        "incident_id": schema.get("incident_id") or schema.get("id") or schema.get("job_id"),
        "expected": schema.get("expected"),
    }
    jobs_path = brd_path.with_name(f"{brd_path.stem}_jobs.json")
    done_file = jobs_path.with_name(f"done_{jobs_path.stem}.json")

    # [1] Handcrafted implementation.
    handcrafted = saved_func_dir / f"{brd_path.stem}_handcrafted"
    if handcrafted.exists() and not skip_reuse_once:
        logger.info("Found handcrafted dir %s", handcrafted)
        try:
            py_files = sorted(handcrafted.glob("*.py"))
            if not py_files:
                raise RuntimeError(f"No .py files in {handcrafted}")
            if len(py_files) > 1:
                raise RuntimeError(f"Expected exactly one .py file in {handcrafted}; found {len(py_files)}")
            py_path = py_files[0]
            script_name = py_path.name
            if is_quarantined(registry_path, hash_signature, script_name):
                logger.info("Handcrafted impl %s is quarantined; skipping.", script_name)
            else:
                hc_mod = load_script(namespace=f"hc_{hash_signature}", py_path=py_path)
                if not hasattr(hc_mod, expected_function_name):
                    raise RuntimeError(f"Handcrafted module must define {expected_function_name}()")
                hc_func = getattr(hc_mod, expected_function_name)
                ok_brd, _ = run_brd_tests(hc_func, brd_tests, contract)
                if ok_brd:
                    ok_deploy, deploy_reflection = run_deployment_validation_jobs(
                        hc_func, jobs_schema, deployment_jobs, contract["output"]
                    )
                    if ok_deploy:
                        logger.info(
                            "✅ Using handcrafted %s() after passing %d labeled deployment job(s)",
                            expected_function_name, n_items_to_pass,
                        )
                        return hc_func, expected_function_name, {"origin": "handcrafted", "script_name": script_name}
                    logger.warning("Handcrafted impl failed labeled deployment validation: %s", deploy_reflection)
                else:
                    logger.warning("Handcrafted impl failed BRD-authored tests/contract")
        except Exception as exc:
            logger.warning("Handcrafted load/test failed: %s", exc)
    elif handcrafted.exists() and skip_reuse_once:
        logger.info("Previous batch failure -> skipping handcrafted reuse this loop.")

    # [2] Cached implementation.
    registry = load_registry(registry_path)
    meta = registry.get(hash_signature, {})
    latest_script = meta.get("latest_script") if isinstance(meta, dict) else None
    if latest_script and not skip_reuse_once:
        func_name = meta.get("function_name") or expected_function_name
        script_name = latest_script
        logger.info("Expected function name: %s() (cache)", func_name)
        try:
            if is_quarantined(registry_path, hash_signature, script_name):
                logger.info("Cached impl %s is quarantined; skipping.", script_name)
            else:
                cached_mod = load_script(
                    namespace=f"saved_{hash_signature}",
                    saved_func_dir=saved_func_dir,
                    hash_signature=hash_signature,
                    script_name=script_name,
                    registry_path=registry_path,
                )
                if not hasattr(cached_mod, func_name):
                    raise RuntimeError(f"Cached module missing {func_name}()")
                cached_func = getattr(cached_mod, func_name)
                ok_brd, _ = run_brd_tests(cached_func, brd_tests, contract)
                if ok_brd:
                    ok_deploy, deploy_reflection = run_deployment_validation_jobs(
                        cached_func, jobs_schema, deployment_jobs, contract["output"]
                    )
                    if ok_deploy:
                        logger.info(
                            "✨ Reusing cached implementation after %d labeled deployment job(s): %s()",
                            n_items_to_pass, func_name,
                        )
                        return cached_func, func_name, {"origin": "cached", "script_name": script_name}
                    logger.info("Cached impl failed labeled deployment validation: %s", deploy_reflection)
                else:
                    logger.info("Cache invalid – BRD-authored tests/contract failed")
        except Exception as exc:
            logger.warning("Cache load/test failed: %s", exc)
    elif latest_script and skip_reuse_once:
        logger.info("Previous batch failure -> skipping cached reuse this loop.")

    # [3] GPT generation. GPT writes code only; Python owns inspection.
    logger.info("🤖 Proceeding to GPT generation...")
    generation_contract = {
        "function_name": expected_function_name,
        "input": contract["input"],
        "output": contract["output"],
    }
    schema_lock_header = (
        "🚨 USE THIS FUNCTION CONTRACT EXACTLY - do not add/remove parameters or alter output shape! 🚨\n"
        f"{json.dumps(generation_contract, ensure_ascii=False, separators=(',', ':'))}\n\n"
    )

    local_reflection = reflection
    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info("Attempt %d/%d for %s()", attempt, MAX_ATTEMPTS, expected_function_name)
        try:
            resp = ask_gpt_with_naming_convention_to_make_func(
                gpt_semaphore,
                schema_lock_header + brd_text,
                expected_function_name,
                param_names,
                local_reflection,
            )
            logger.info("GPT response received - length: %d chars", len(resp))
            if is_debug_mode:
                logger.info("GPT response:\n%s", resp)
        except Exception as exc:
            local_reflection = f"GPT call failed with error: {type(exc).__name__}: {exc}"
            logger.warning("Attempt %d failed at GPT call: %s", attempt, exc)
            continue

        try:
            code = clean_generated_code(resp)
        except Exception as exc:
            local_reflection = f"Return executable Python source code only. {type(exc).__name__}: {exc}"
            logger.warning("Attempt %d failed at code parsing: %s", attempt, exc)
            continue

        try:
            ns = {"my_tools": my_tools}
            exec(code, ns)
            func = ns.get(expected_function_name)
            if not callable(func):
                raise ValueError(f"Function {expected_function_name} not found or not callable")
            logger.info("Code compilation successful - function %s loaded", expected_function_name)
        except Exception as exc:
            local_reflection = f"Your code did not compile:\n{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}"
            logger.warning("Attempt %d failed at compilation: %s", attempt, exc)
            continue

        ok_brd, feedback = run_brd_tests(func, brd_tests, contract)
        if not ok_brd:
            failed = "\n".join([ln for ln in feedback if " -> OK " not in ln][:8])
            local_reflection = (
                "Your function failed the user's BRD contract/tests as executed by Python. "
                "Do not change or reinterpret the contract or expected outputs. Fix only the implementation.\n"
                f"{failed}"
            )
            logger.warning("Attempt %d failed BRD-authored tests/contract", attempt)
            continue

        # Deployment gate: exactly the first N labeled jobs selected by Python.
        ok_deploy, reflection2 = run_deployment_validation_jobs(
            func, jobs_schema, deployment_jobs, contract["output"]
        )
        if not ok_deploy:
            logger.warning(
                "Attempt %d failed one of the first %d labeled deployment jobs; retrying with reflection",
                attempt, n_items_to_pass,
            )
            local_reflection = reflection2
            continue

        logger.info(
            "✅ Success on attempt %d after BRD tests + %d labeled deployment job(s)!",
            attempt, n_items_to_pass,
        )
        logger.info("🔍 Saving function with hash: %s", hash_signature)
        saved_name = save_source(
            hash_signature, code, expected_function_name, brd_path,
            saved_func_dir, registry_path, registry_lock,
        )
        logger.info("💾 %s Function saved successfully!", expected_function_name)
        return func, expected_function_name, {"origin": "gpt", "script_name": saved_name}

    logger.error("❌ All %d attempts failed for %s", MAX_ATTEMPTS, brd_path.name)
    return None, None, None

# --- Quarantine helpers (add near save_registry/load_registry) ---
def _reg_get(reg: dict, key: str, default):
    v = reg.get(key)
    return v if isinstance(v, type(default)) else default

def is_quarantined(reg_path: pathlib.Path, hash_signature: str, script_name: str) -> bool:
    reg = load_registry(reg_path)
    meta = reg.get(hash_signature, {})
    q = _reg_get(meta, "quarantine", [])
    for item in q:
        # match by script name only; keep origin as metadata
        if item.get("script") == script_name:
            return True
    return False

def add_quarantine(reg_path: pathlib.Path, hash_signature: str, script_name: str, origin: str,
                   reason: str, registry_lock=None) -> None:
    def _update_registry():
        reg = load_registry(reg_path)
        meta = reg.setdefault(hash_signature, {})
        q = _reg_get(meta, "quarantine", [])

        if any(it.get("script") == script_name for it in q):
            return

        q.append({
            "script": script_name,
            "origin": origin,
            "reason": (reason or "")[:400],
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        })
        meta["quarantine"] = q
        save_registry(reg_path, reg)

    if registry_lock is None:
        _update_registry()
    else:
        with registry_lock:
            _update_registry()


###############################   Main Agent fucntion ######################
def process_single_brd_standalone(
    brd_path: pathlib.Path,
    saved_func_dir: pathlib.Path,
    registry_path: pathlib.Path,
    gpt_semaphore: multiprocessing.Semaphore,
    log_queue: multiprocessing.Queue,
    shared_data,
    is_debug_mode: bool = False,
    check_interval: int = 5,
) -> None:
    """
    Perpetually process a single BRD, checking for new jobs at regular intervals.
    Exits only on: per-BRD stop flag, signals, parent death, or IPC failure.

    CHANGE: After MAX_ATTEMPTS failures to generate a function for a given BRD hash,
    enter a "blocked" state that only logs a periodic warning until the BRD changes.
    """
    import signal, os, psutil
    import logging
    import logging.handlers

    # ---------- local constants (no dependency on job_manager.py) ----------
    WORKER_MAX_RECORD_BYTES = 64_000
    hb_max_age = HB_MAX_AGE
    hb_misses_before_exit = HB_MISSES_BEFORE_EXIT
    warn_bad_BRD_every_x_seconds = 20  # only used later; keep here for readability

    # Make tool references available to worker code
    my_tools.gpt_semaphore = gpt_semaphore
    my_tools.shared_data = shared_data
    registry_lock = shared_data.get("lock") if hasattr(shared_data, "get") else None

    # ---------- worker-safe queue handler (NO formatting in worker) ----------
    class _WorkerQueueHandler(logging.handlers.QueueHandler):
        def __init__(self, q, max_bytes: int = WORKER_MAX_RECORD_BYTES):
            super().__init__(q)
            self._dead = False
            self._max = max_bytes

        def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
            # Always render the message now; args can contain objects that cannot be pickled.
            record.msg = record.getMessage()
            record.args = None

            # Traceback objects are not picklable. Preserve ERROR+ tracebacks as plain text.
            if record.exc_info:
                if record.levelno >= logging.ERROR and not record.exc_text:
                    record.exc_text = logging.Formatter().formatException(record.exc_info)
                record.exc_info = None
            if record.levelno < logging.ERROR:
                record.exc_text = None
                record.stack_info = None

            if isinstance(record.msg, str) and len(record.msg) > self._max:
                record.msg = record.msg[: self._max] + " …<truncated>"
            return record

        def emit(self, record: logging.LogRecord) -> None:
            if self._dead:
                return
            try:
                self.enqueue(self.prepare(record))
            except queue.Full:
                pass  # transient back-pressure: drop only this record, keep the handler alive
            except Exception:
                self._dead = True  # permanent IPC failure: fuse off quietly

        def close(self) -> None:
            self._dead = True
            try:
                super().close()
            except Exception:
                pass

    # ---------- logging hookup: ONE handler, multiple loggers ----------
    worker_name = f"JobWorker.{brd_path.stem}"

    # The per-worker logger (lifecycle + high-level events)
    worker_logger = logging.getLogger(worker_name)
    worker_logger.setLevel(logging.DEBUG if is_debug_mode else logging.INFO)
    worker_logger.propagate = False

    # Module logger used across this file (stable name for uniform logs)
    module_logger = logging.getLogger("brd_processor")
    module_logger.setLevel(logging.DEBUG if is_debug_mode else logging.INFO)
    module_logger.propagate = False

    # Logger exposed to generated code via my_tools.log
    tools_logger = logging.getLogger(f"{worker_name}.my_tools")
    tools_logger.setLevel(logging.DEBUG if is_debug_mode else logging.INFO)
    tools_logger.propagate = False
    my_tools.log = tools_logger  # rebind toolbox logger for generated code

    # Create ONE queue handler (NO formatter here; manager formats)
    qh = _WorkerQueueHandler(log_queue)

    # Remove any stale handlers (important when workers restart)
    for lg in (worker_logger, module_logger, tools_logger):
        for h in list(lg.handlers):
            try:
                lg.removeHandler(h)
                h.close()
            except Exception:
                pass
        lg.addHandler(qh)

    # Quiet noisy third-party libs (levels only; no handlers added here)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # ---------- signals ----------
    shutdown_requested = False
    def _on_signal(signum, frame):
        nonlocal shutdown_requested
        try: worker_logger.info("Received signal %d — shutting down %s", signum, brd_path.name)
        except Exception: pass
        shutdown_requested = True
    for s in (getattr(signal, "SIGTERM", None),
              getattr(signal, "SIGINT", None),
              getattr(signal, "SIGHUP", None)):
        if s:
            try: signal.signal(s, _on_signal)
            except Exception: pass

    # ---------- runtime state ----------
    parent_pid = os.getppid()
    heartbeat_interval = 10.0
    last_heartbeat_check = time.time()
    last_activity_time = time.time()
    max_idle_time = None  # run forever unless set
    hb_miss_count = 0
    last_jobs_state = None
    cached_func = None
    cached_schema = None
    cached_contract = None
    cached_brd_tests = None
    cached_n_items_to_pass = None
    last_brd_hash = None
    last_reflection = None
    last_impl_meta = None

    # Blocked state: after repeated failures for a specific BRD hash
    blocked_hash: str | None = None
    blocked_reason: str | None = None
    next_block_warn_ts: float = 0.0  # throttle warnings to 20s
    schema_fail_count = 0  # consecutive schema-extraction failures -> exponential backoff
    next_deployment_preflight_warn_ts = 0.0

    def _should_stop() -> bool:
        if shutdown_requested:
            return True
        try:
            sd = shared_data.get("shared_dict") if hasattr(shared_data, "get") else shared_data["shared_dict"]
            if sd.get(f"stop::{brd_path.name}", False) or sd.get("shutdown_requested", False):
                return True
        except (BrokenPipeError, EOFError, OSError):
            return True
        except Exception:
            pass
        try:
            return (not psutil.pid_exists(parent_pid)) or os.getppid() != parent_pid
        except Exception:
            return True

    def _wait_interruptibly(seconds: float) -> bool:
        """Wait up to *seconds*. Return True if shutdown/parent loss is detected."""
        deadline = time.time() + max(0.0, seconds)
        while True:
            if _should_stop():
                return True
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            time.sleep(min(0.5, remaining))

    worker_logger.info(
        "Starting perpetual processing for %s (PID=%d, PPID=%d, interval=%ds, debug=%s)",
        brd_path.name, os.getpid(), parent_pid, check_interval, is_debug_mode
    )

    try:
        while not shutdown_requested:
            try:
                now = time.time()

                # per-BRD stop flag / global shutdown
                try:
                    if isinstance(shared_data, dict) and "shared_dict" in shared_data:
                        sd = shared_data["shared_dict"]
                        if sd.get(f"stop::{brd_path.name}", False):
                            worker_logger.info("Stop flag detected for %s — exiting.", brd_path.name)
                            break
                        if sd.get("shutdown_requested", False):
                            worker_logger.info("Global shutdown flag detected — exiting %s.", brd_path.name)
                            break
                except Exception:
                    pass

                # parent-alive check
                if now - last_heartbeat_check >= heartbeat_interval:
                    try:
                        if not psutil.pid_exists(parent_pid) or os.getppid() != parent_pid:
                            worker_logger.info("Parent appears gone — exiting %s.", brd_path.name)
                            break
                    except Exception:
                        worker_logger.info("Parent check failed — assuming parent gone; exiting %s.", brd_path.name)
                        break
                    finally:
                        last_heartbeat_check = now

                # manager heartbeat
                ok_hb, hb_reason = _check_manager_heartbeat_shared(shared_data, hb_max_age)
                if not ok_hb:
                    hb_miss_count += 1
                    if hb_miss_count == 1:
                        worker_logger.warning("Manager heartbeat check failed (%s) — will recheck.", hb_reason)
                    if hb_miss_count >= hb_misses_before_exit:
                        worker_logger.info("Manager heartbeat stale after %d checks — exiting %s.",
                                           hb_miss_count, brd_path.name)
                        break
                else:
                    if hb_miss_count:
                        worker_logger.info("Manager heartbeat recovered after %d misses (%s).",
                                           hb_miss_count, hb_reason)
                    hb_miss_count = 0

                # idle timeout (disabled)
                if max_idle_time and (now - last_activity_time > max_idle_time):
                    worker_logger.info("Idle timeout (>%ss) — exiting %s.", max_idle_time, brd_path.name)
                    break

                # ----------------- BRD read & hash -----------------
                try:
                    brd_text = brd_path.read_text(encoding="utf-8")
                    current_hash = brd_hash_signature(brd_text)
                except FileNotFoundError:
                    worker_logger.info("BRD file %s deleted mid-loop — exiting worker.", brd_path.name)
                    break
                except Exception as exc:
                    module_logger.error("Failed to read BRD %s: %s", brd_path.name, exc, exc_info=is_debug_mode)
                    time.sleep(check_interval)
                    continue

                # if we were blocked and the BRD changed, unblock
                brd_changed = (last_brd_hash != current_hash)
                if brd_changed:
                    blocked_hash = None
                    blocked_reason = None
                    next_block_warn_ts = 0.0
                    schema_fail_count = 0
                    worker_logger.info("BRD content changed — invalidating cache for %s", brd_path.name)
                    cached_func = None
                    cached_schema = None
                    cached_contract = None
                    cached_brd_tests = None
                    cached_n_items_to_pass = None
                    next_deployment_preflight_warn_ts = 0.0
                    last_reflection = None
                    last_impl_meta = None
                    last_brd_hash = current_hash
                    last_activity_time = now

                # -------------- BLOCKED MODE: no GPT calls, warn every x seconds --------------
                if blocked_hash == current_hash:
                    if now >= next_block_warn_ts:
                        module_logger.warning(
                            "BRD %s is blocked: %s. Waiting for BRD to change.",
                            brd_path.name, blocked_reason or "no working function",
                        )
                        next_block_warn_ts = now + warn_bad_BRD_every_x_seconds
                    time.sleep(min(5.0, check_interval * 2))
                    continue

                # ----------------- Python-owned BRD contract/tests -----------------
                if cached_contract is None or cached_brd_tests is None or cached_n_items_to_pass is None:
                    try:
                        contract = extract_brd_contract(brd_text)
                        brd_tests = extract_brd_tests(brd_text, contract)
                        n_items_to_pass = extract_n_items_to_pass(brd_text)
                        cached_contract = contract
                        cached_brd_tests = brd_tests
                        cached_n_items_to_pass = n_items_to_pass
                        module_logger.info(
                            "Parsed BRD inspector data for %s: inputs=%s tests=%d N_ITEMS_TO_PASS=%d",
                            brd_path.name, [item["name"] for item in contract["input"]],
                            len(brd_tests), n_items_to_pass,
                        )
                    except Exception as exc:
                        blocked_hash = current_hash
                        blocked_reason = (
                            "invalid FUNCTION INPUT/OUTPUT CONTRACT, TEST EXAMPLES, or JOBS DATA STRUCTURE "
                            f"inspector data ({exc})"
                        )
                        next_block_warn_ts = 0.0
                        module_logger.error("BRD inspector data invalid for %s: %s", brd_path.name, exc)
                        time.sleep(check_interval)
                        continue
                else:
                    contract = cached_contract
                    brd_tests = cached_brd_tests
                    n_items_to_pass = cached_n_items_to_pass

                # ----------------- Job metadata / function name (cache) -----------------
                if cached_schema is None:
                    try:
                        schema = gpt_extract_function_and_data_schema(brd_text, gpt_semaphore)
                        schema["inputs"] = [item["name"] for item in contract["input"]]
                        cached_schema = schema
                        schema_fail_count = 0
                        module_logger.info("Function/job metadata extracted for %s: %s", brd_path.name,
                                           schema.get("function_name", "<unknown>"))
                        if is_debug_mode:
                            module_logger.debug("Schema JSON: %s", json.dumps(schema, ensure_ascii=False))
                        last_activity_time = now
                    except Exception as exc:
                        schema_fail_count += 1
                        exponent = min(schema_fail_count - 1, 10)
                        backoff = min(float(check_interval) * (2 ** exponent), 600.0)
                        module_logger.error(
                            "Schema extraction failed for %s (attempt %d, next retry in %.0fs): %s",
                            brd_path.name, schema_fail_count, backoff, exc, exc_info=is_debug_mode,
                        )
                        if _wait_interruptibly(backoff):
                            worker_logger.info("Stop detected during schema retry backoff — exiting %s.", brd_path.name)
                            break
                        continue
                else:
                    schema = cached_schema
                    schema["inputs"] = [item["name"] for item in contract["input"]]

                # ----------------- Deployment-data preflight + ensure function -----------------
                if cached_func is None:
                    jobs_schema_for_deploy = {
                        "inputs": [item["name"] for item in contract["input"]],
                        "expected": schema.get("expected"),
                        "incident_id": schema.get("incident_id") or schema.get("id") or schema.get("job_id"),
                    }
                    jobs_path_for_deploy = brd_path.with_name(f"{brd_path.stem}_jobs.json")

                    try:
                        deployment_jobs, labeled_count = prepare_deployment_validation_jobs(
                            jobs_path_for_deploy, jobs_schema_for_deploy, contract, n_items_to_pass
                        )
                        next_deployment_preflight_warn_ts = 0.0
                        module_logger.info(
                            "Deployment preflight for %s: using first %d labeled job(s) out of %d labeled",
                            brd_path.name, n_items_to_pass, labeled_count,
                        )
                    except Exception as exc:
                        # Do not call the implementation-generating LLM while deployment is impossible.
                        # We intentionally keep the worker alive and recheck the jobs file; exiting here
                        # would make the current JobManager immediately restart the worker in a loop.
                        if now >= next_deployment_preflight_warn_ts:
                            module_logger.warning(
                                "Deployment preflight blocked for %s: %s. No function will be generated/deployed "
                                "until the jobs data satisfies N_ITEMS_TO_PASS=%d.",
                                brd_path.name, exc, n_items_to_pass,
                            )
                            next_deployment_preflight_warn_ts = now + warn_bad_BRD_every_x_seconds
                        time.sleep(min(5.0, check_interval * 2))
                        continue

                    func, func_name, impl_meta = _get_or_create_function(
                        brd_path=brd_path,
                        brd_text=brd_text,
                        hash_signature=current_hash,
                        schema=schema,
                        contract=contract,
                        brd_tests=brd_tests,
                        n_items_to_pass=n_items_to_pass,
                        deployment_jobs=deployment_jobs,
                        saved_func_dir=saved_func_dir,
                        registry_path=registry_path,
                        gpt_semaphore=gpt_semaphore,
                        registry_lock=registry_lock,
                        logger=module_logger,
                        is_debug_mode=is_debug_mode,
                        reflection=last_reflection,
                    )
                    if func is None:
                        # Enter blocked mode for this BRD hash
                        blocked_hash = current_hash
                        blocked_reason = f"no working function after up to {MAX_ATTEMPTS} generation attempts; see earlier ERROR"
                        next_block_warn_ts = 0.0
                        module_logger.warning(
                            "Entering blocked mode for %s: %s. Will warn every %ss until BRD changes.",
                            brd_path.name, blocked_reason, warn_bad_BRD_every_x_seconds,
                        )
                        time.sleep(check_interval * 2)
                        continue

                    cached_func = func
                    last_impl_meta = impl_meta or {"origin": "unknown", "script_name": "<unknown>"}
                    last_activity_time = now

                # ----------------- Jobs derivation -----------------
                jobs_schema = {
                    "inputs": [item["name"] for item in contract["input"]],
                    "expected": schema.get("expected"),
                    "incident_id": schema.get("incident_id") or schema.get("id") or schema.get("job_id"),
                }

                jobs_path = brd_path.with_name(f"{brd_path.stem}_jobs.json")
                done_file = jobs_path.with_name(f"done_{jobs_path.stem}.json")

                # ----------------- Check for NEW jobs -----------------
                has_new_jobs = False
                new_jobs = []
                total_jobs = 0

                if jobs_path.exists():
                    try:
                        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
                        if isinstance(jobs, list) and jobs:
                            total_jobs = len(jobs)
                            id_field = jobs_schema["incident_id"]
                            if not id_field:
                                raise ValueError("schema has no incident_id field; cannot track processed jobs")
                            done_ids = _read_done_ids(done_file)
                            new_jobs = [rec for rec in jobs if str(rec.get(id_field)) not in done_ids]
                            has_new_jobs = bool(new_jobs)

                            if not has_new_jobs and last_jobs_state != "idle":
                                module_logger.info("No new jobs for %s — all done.", brd_path.name)
                                last_jobs_state = "idle"
                            elif has_new_jobs and last_jobs_state != "pending":
                                module_logger.info("%d new job(s) for %s (done %d/%d).",
                                                   len(new_jobs), brd_path.name, total_jobs - len(new_jobs), total_jobs)
                                if is_debug_mode:
                                    module_logger.debug("First job preview: %s", json.dumps(new_jobs[0], ensure_ascii=False)[:800])
                                last_jobs_state = "pending"
                        else:
                            if last_jobs_state != "idle":
                                module_logger.info("No new jobs for %s — jobs file empty.", brd_path.name)
                                last_jobs_state = "idle"
                            has_new_jobs = False
                    except Exception as exc:
                        module_logger.error("Error checking jobs for %s: %s", brd_path.name, exc, exc_info=is_debug_mode)
                        time.sleep(check_interval)
                        continue
                else:
                    if last_jobs_state != "idle":
                        module_logger.info("No jobs file for %s yet (%s).", brd_path.name, jobs_path.name)
                        last_jobs_state = "idle"
                    has_new_jobs = False

                if not has_new_jobs:
                    time.sleep(min(5.0, check_interval * 2))
                    continue

                # ----------------- Process jobs (if any) -----------------
                ok_batch, reflection = process_jobs(
                    cached_func, jobs_schema, jobs_path, done_file,
                    output_contract=contract["output"],
                    compare_expected=False,
                )
                if ok_batch:
                    worker_logger.info("✅ Jobs processed for %s", brd_path.name)
                    last_reflection = None
                    last_activity_time = now
                    time.sleep(check_interval)
                else:
                    worker_logger.warning("❌ Batch failed for %s: %s", brd_path.name, reflection)
                    # --- Quarantine the implementation that just failed (suggestion #3) ---
                    try:
                        if last_impl_meta and last_brd_hash == current_hash:
                            add_quarantine(
                                reg_path=registry_path,
                                hash_signature=current_hash,
                                script_name=last_impl_meta.get("script_name", "<unknown>"),
                                origin=last_impl_meta.get("origin", "unknown"),
                                reason=(reflection or "batch failed"),
                                registry_lock=registry_lock,
                            )
                            module_logger.info("Quarantined %s (%s) for BRD hash %s",
                                               last_impl_meta.get("script_name"), last_impl_meta.get("origin"), current_hash)
                    except Exception as q_exc:
                        module_logger.warning("Failed to quarantine impl: %s", q_exc)

                    last_reflection = reflection
                    cached_func = None          # force regeneration next loop
                    last_impl_meta = None
                    time.sleep(check_interval)

            except (BrokenPipeError, EOFError, OSError) as exc:
                try: worker_logger.info("IPC channel gone (%s) — exiting %s.", type(exc).__name__, brd_path.name)
                except Exception: pass
                break
            except KeyboardInterrupt:
                worker_logger.info("KeyboardInterrupt — exiting %s", brd_path.name)
                break
            except Exception as exc:
                module_logger.exception("Loop error for %s (will retry): %s", brd_path.name, exc)
                cached_func = None
                cached_schema = None
                cached_contract = None
                cached_brd_tests = None
                cached_n_items_to_pass = None
                time.sleep(check_interval)

    except Exception as exc:
        module_logger.exception("Fatal error in perpetual processing for %s: %s", brd_path.name, exc)
        raise
    finally:
        # Final summary, then detach handler from all loggers and close once
        try:
            mem_mb = psutil.Process().memory_info().rss / (1024 * 1024)
            runtime = time.time() - last_activity_time
            worker_logger.info(
                "Perpetual processing stopped for %s (Δt since last activity: %.1fs, RSS: %.1f MB)",
                brd_path.name, runtime, mem_mb
            )
        except Exception:
            pass
        try:
            import gc; gc.collect()
        except Exception:
            pass
        for lg in (worker_logger, module_logger, tools_logger):
            try: lg.removeHandler(qh)
            except Exception: pass
        try: qh.close()
        except Exception: pass
        try: logging.shutdown()
        except Exception: pass