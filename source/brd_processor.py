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
import ast
import logging, sys

_RETRY_STEPS = 6
MAX_ATTEMPTS = 10
MAX_BATCH_FAILURES = 3
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

_active_gpt_semaphore = None

# Synthetic module exposed to generated functions. Keep this surface deliberately small.
import types
toolbox = types.ModuleType("my_tools")
toolbox.log = logging.getLogger("my_tools")
toolbox.gpt_model = AZURE_OPENAI_DEPLOYMENT
toolbox.ask_gpt = None
sys.modules["my_tools"] = toolbox
import my_tools

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
    """Extract only the Python function name.

    Python, not the LLM, parses the function I/O contract, tests, ID_FIELD,
    EXPECTED_FIELD, and N_ITEMS_TO_PASS from rigid BRD sections.
    """
    SYSTEM = (
        "You are a technical requirements analyst.\n"
        "Read the Business Requirement Document (BRD).\n"
        "Python separately parses all trust-critical contracts, tests, and job metadata.\n"
        "Choose only the Python function name that best represents the BRD task.\n"
        "Return exactly ONE line of valid JSON with exactly one key:\n"
        '  {"function_name":"snake_case_python_identifier"}\n'
        "No markdown, prose, comments, or additional keys."
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
        raise ValueError(f"GPT function-name JSON invalid: {exc}\n---\n{txt}") from exc
    if not isinstance(schema, dict) or set(schema) != {"function_name"}:
        raise ValueError(f"GPT function-name JSON must contain only function_name: {schema!r}")
    function_name = schema["function_name"]
    if not isinstance(function_name, str) or not _ID_RE.fullmatch(function_name):
        raise ValueError(f"GPT returned invalid Python function_name: {function_name!r}")
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
Do not use os, sys, subprocesses, sockets, raw network clients, eval/exec/compile,
direct file-opening builtins, dynamic attribute introspection, or private/dunder attributes.
If the BRD requires an LLM call, use my_tools.ask_gpt(messages, temperature=...);
never access an authenticated model client directly.
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


_FORBIDDEN_IMPORT_ROOTS = {
    "os", "sys", "subprocess", "socket", "requests", "httpx", "urllib", "ftplib",
    "shutil", "pathlib", "importlib", "builtins", "ctypes", "pickle", "marshal",
    "inspect", "resource", "signal", "openai",
}
_FORBIDDEN_NAMES = {
    "exec", "eval", "compile", "open", "__import__", "globals", "locals", "vars",
    "getattr", "setattr", "delattr", "breakpoint", "input", "help", "exit", "quit",
    "__builtins__",
}
_FORBIDDEN_TOOL_ATTRS = {"gpt_client", "shared_data", "gpt_semaphore"}


def _static_safety_check(code: str) -> None:
    """Reject obvious dangerous constructs before generated/cached code executes.

    This is defense-in-depth, not a sandbox. Generated code still needs process
    isolation/timeouts for a strong security boundary.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise ValueError(f"code does not parse: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in _FORBIDDEN_IMPORT_ROOTS:
                    raise ValueError(f"line {node.lineno}: import of {alias.name!r} is not allowed")
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".")[0]
            if node.level or root in _FORBIDDEN_IMPORT_ROOTS:
                raise ValueError(f"line {node.lineno}: import from {node.module!r} is not allowed")
            if node.module == "my_tools":
                for alias in node.names:
                    if alias.name == "*" or alias.name in _FORBIDDEN_TOOL_ATTRS:
                        raise ValueError(f"line {node.lineno}: importing {alias.name!r} from my_tools is not allowed")
        elif isinstance(node, ast.Name) and node.id in _FORBIDDEN_NAMES:
            raise ValueError(f"line {node.lineno}: use of {node.id!r} is not allowed")
        elif isinstance(node, ast.Attribute):
            if node.attr.startswith("_"):
                raise ValueError(f"line {node.lineno}: private/dunder attribute {node.attr!r} is not allowed")
            if node.attr in _FORBIDDEN_TOOL_ATTRS:
                raise ValueError(f"line {node.lineno}: attribute {node.attr!r} is not available to generated code")


def _extract_section_body(brd_text: str, heading: str) -> str:
    """Return a rigid BRD section body by matching a true heading line."""
    lines = brd_text.splitlines()
    wanted = heading.strip().upper()

    def _normalized_heading(line: str) -> str:
        text = line.strip()
        match = re.fullmatch(r"\d+\.\s+(.+)", text)
        if match:
            text = match.group(1).strip()
        return text.rstrip(":").strip().upper()

    def _is_numbered_section_heading(index: int) -> bool:
        if not re.fullmatch(r"\d+\.\s+\S.*", lines[index].strip()):
            return False
        next_index = index + 1
        while next_index < len(lines) and not lines[next_index].strip():
            next_index += 1
        return next_index < len(lines) and bool(re.fullmatch(r"[-=]{3,}", lines[next_index].strip()))

    start = None
    for i, line in enumerate(lines):
        if _normalized_heading(line) == wanted:
            start = i + 1
            break
    if start is None:
        raise ValueError(f"BRD is missing required section: {heading}")

    end = len(lines)
    for i in range(start, len(lines)):
        stripped = lines[i].strip()
        if "TECHNICAL DESCRIPTIONS" in stripped.upper() or _is_numbered_section_heading(i):
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
_ID_FIELD_RE = re.compile(r"^ID_FIELD=([A-Za-z_][A-Za-z0-9_]*)$")
_EXPECTED_FIELD_RE = re.compile(r"^EXPECTED_FIELD=([A-Za-z_][A-Za-z0-9_]*)$")


def extract_job_directives(brd_text: str) -> dict[str, Any]:
    """Parse trust-critical job metadata directly from JOBS DATA STRUCTURE."""
    body = _extract_section_body(brd_text, "JOBS DATA STRUCTURE")
    patterns = {
        "N_ITEMS_TO_PASS": _N_ITEMS_TO_PASS_RE,
        "ID_FIELD": _ID_FIELD_RE,
        "EXPECTED_FIELD": _EXPECTED_FIELD_RE,
    }
    values: dict[str, list[Any]] = {key: [] for key in patterns}
    malformed: list[str] = []

    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        for key, pattern in patterns.items():
            if line.startswith(key):
                match = pattern.fullmatch(line)
                if match is None:
                    malformed.append(line)
                else:
                    value: Any = match.group(1)
                    if key == "N_ITEMS_TO_PASS":
                        value = int(value)
                    values[key].append(value)
                break

    if malformed:
        raise ValueError(
            "JOBS DATA STRUCTURE contains malformed directive line(s): "
            f"{malformed!r}. Required forms are ID_FIELD=<identifier>, "
            "EXPECTED_FIELD=<identifier>, and N_ITEMS_TO_PASS=<positive integer>."
        )

    missing = [key for key, found in values.items() if not found]
    duplicates = [key for key, found in values.items() if len(found) > 1]
    if missing or duplicates:
        raise ValueError(
            "JOBS DATA STRUCTURE must contain exactly one standalone line for each of "
            "ID_FIELD=<identifier>, EXPECTED_FIELD=<identifier>, and "
            f"N_ITEMS_TO_PASS=<positive integer>; missing={missing}, duplicates={duplicates}"
        )

    return {
        "id_field": values["ID_FIELD"][0],
        "expected_field": values["EXPECTED_FIELD"][0],
        "n_items_to_pass": values["N_ITEMS_TO_PASS"][0],
    }


def extract_n_items_to_pass(brd_text: str) -> int:
    """Backward-compatible helper returning the Python-parsed deployment count."""
    return int(extract_job_directives(brd_text)["n_items_to_pass"])



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

    call_kwargs = dict(kwargs)
    backoff = 1.0
    for _ in range(_RETRY_STEPS):
        try:
            with semaphore:
                return gpt_client.chat.completions.create(model=my_tools.gpt_model, **call_kwargs)
        except (RateLimitError, APIStatusError) as exc:
            status = getattr(exc, "status_code", None)
            message = str(exc).lower()
            if status == 400 and "temperature" in call_kwargs and "temperature" in message:
                call_kwargs.pop("temperature", None)
                LOG.warning("Deployment rejected temperature; retrying without it.")
                continue
            if status != 429:
                LOG.exception("GPT hard error")
                raise

        sleep = backoff + random.uniform(0, 0.3)
        LOG.warning("GPT 429 - retry in %.1fs", sleep)
        time.sleep(sleep)  # semaphore is intentionally released during backoff
        backoff = min(backoff * 2, 60)
    raise RuntimeError("GPT call failed after retries")


def _generated_ask_gpt(messages, temperature=0):
    """Narrow LLM wrapper exposed to generated functions; returns message text only."""
    if _active_gpt_semaphore is None:
        raise RuntimeError("Generated-code GPT tool is unavailable outside an active BRD worker")
    kwargs = {"messages": messages}
    if temperature is not None:
        kwargs["temperature"] = temperature
    resp = gpt_call_with_retry(_active_gpt_semaphore, **kwargs)
    return resp.choices[0].message.content


toolbox.ask_gpt = _generated_ask_gpt



def _read_done_ids(done_file: pathlib.Path) -> set[str]:
    try:
        data = json.loads(done_file.read_text(encoding="utf-8"))
        return set(data["ids"])
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return set()


def _read_rejected_hashes(done_file: pathlib.Path) -> set[str]:
    try:
        data = json.loads(done_file.read_text(encoding="utf-8"))
        rejected = data.get("rejected", {})
        return set(rejected) if isinstance(rejected, dict) else set()
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def _runtime_record_hash(record: Any) -> str:
    payload = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2s(payload.encode("utf-8"), digest_size=16).hexdigest()


def process_jobs(
    func: Callable[..., Any],
    jobs_schema: Dict[str, Any],
    jobs_path: pathlib.Path,
    done_file: Optional[pathlib.Path] = None,
    *,
    output_contract: dict[str, Any] | None = None,
    input_specs: list[dict[str, Any]] | None = None,
    results_file: Optional[pathlib.Path] = None,
    preview_limit: int = 1200,
    compare_expected: bool = True,
) -> Tuple[bool, Optional[str]]:
    """Run runtime jobs with BRD input/output validation and durable progress state."""
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
    if not id_field:
        raise ValueError("Runtime jobs require a Python-parsed ID_FIELD")

    if done_file is None:
        done_file = jobs_path.parent / f"done_{jobs_path.stem}.json"
    if results_file is None:
        results_file = jobs_path.parent / f"results_{jobs_path.stem}.json"
    done_file.parent.mkdir(parents=True, exist_ok=True)
    results_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        raw_done = json.loads(done_file.read_text(encoding="utf-8"))
        done_ids: Set[str] = set(raw_done.get("ids", []))
        rejected: Dict[str, Any] = dict(raw_done.get("rejected", {}))
    except Exception:
        done_ids = set()
        rejected = {}

    try:
        raw_results = json.loads(results_file.read_text(encoding="utf-8"))
        results: Dict[str, Any] = dict(raw_results.get("results", {}))
    except Exception:
        results = {}

    def _persist_done() -> None:
        try:
            _atomic_write_json(done_file, {"ids": sorted(done_ids), "rejected": rejected})
        except Exception as exc:
            LOG.warning("Could not write done file %s: %s", done_file, exc)

    def _persist_results() -> None:
        try:
            _atomic_write_json(results_file, {"results": results})
        except Exception as exc:
            LOG.warning("Could not write results file %s: %s", results_file, exc)

    new_jobs = []
    for rec in jobs:
        record_hash = _runtime_record_hash(rec)
        if isinstance(rec, dict) and id_field in rec and str(rec[id_field]) in done_ids:
            continue
        if record_hash in rejected:
            continue
        new_jobs.append(rec)

    if not new_jobs:
        LOG.info("All actionable jobs in %s are already processed or rejected.", jobs_path.name)
        return True, None

    LOG.info("Batch start for %s: %d actionable / %d total", jobs_path.name, len(new_jobs), len(jobs))

    for rec in new_jobs:
        record_hash = _runtime_record_hash(rec)
        incident_id = "?"

        def _reject(reason: str) -> None:
            rejected[record_hash] = {"incident_id": incident_id, "reason": reason}
            LOG.warning("incident_id=%s rejected (data validation; function not called): %s", incident_id, reason)
            _persist_done()

        if not isinstance(rec, dict):
            _reject("job record must be a JSON object")
            continue
        if id_field not in rec or rec[id_field] in (None, ""):
            _reject(f"missing or empty ID_FIELD {id_field!r}")
            continue

        incident_id = str(rec[id_field])
        if incident_id in done_ids:
            continue

        if input_specs is not None:
            bad_reason = None
            for spec in input_specs:
                name = spec["name"]
                if name not in rec:
                    bad_reason = f"missing input field {name!r}"
                    break
                ok_in, reason_in = _validate_value_against_contract(rec[name], spec, f"job {incident_id}.{name}")
                if not ok_in:
                    bad_reason = reason_in
                    break
            if bad_reason is not None:
                _reject(bad_reason)
                continue

        if compare_expected and exp_field and exp_field in rec and output_contract is not None:
            ok_expected, expected_reason = _validate_value_against_contract(
                rec[exp_field], output_contract, f"job {incident_id}.{exp_field}"
            )
            if not ok_expected:
                _reject(f"invalid expected-answer field: {expected_reason}")
                continue

        params = [rec[name] for name in param_names]
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
                    _persist_done()
                    _persist_results()
                    return False, reflection

            if compare_expected and exp_field and exp_field in rec:
                expected = rec[exp_field]
                if not _strict_equal(result, expected):
                    reflection = (
                        "Function returned a wrong result on a labeled runtime job.\n\n"
                        f"Inputs passed: {params}\n"
                        f"Expected: {expected!r} (type {type(expected).__name__})\n"
                        f"Got: {result!r} (type {type(result).__name__})"
                    )
                    LOG.warning(
                        "incident_id=%s: got %r (type %s), expected %r (type %s)",
                        incident_id, result, type(result).__name__, expected, type(expected).__name__,
                    )
                    _persist_done()
                    _persist_results()
                    return False, reflection

            LOG.info("incident_id=%s: %s", incident_id, result)
            results[incident_id] = result
            _persist_results()  # persist output before marking the job done
            done_ids.add(incident_id)
            # If a corrected record for this incident succeeds, clear older rejection entries for that ID.
            rejected = {
                key: value for key, value in rejected.items()
                if not isinstance(value, dict) or value.get("incident_id") != incident_id
            }
            _persist_done()

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
            _persist_done()
            _persist_results()
            return False, reflection

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
            if is_quarantined(registry_path, hash_signature, script_name, script_path=py_path):
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
                        return hc_func, expected_function_name, {
                            "origin": "handcrafted", "script_name": script_name, "script_path": str(py_path)
                        }
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
            cached_folder = meta.get("folder_name") or hash_signature
            cached_path = saved_func_dir / cached_folder / script_name
            if is_quarantined(registry_path, hash_signature, script_name, script_path=cached_path):
                logger.info("Cached impl %s is quarantined; skipping.", script_name)
            else:
                _static_safety_check(cached_path.read_text(encoding="utf-8"))
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
                        return cached_func, func_name, {
                            "origin": "cached", "script_name": script_name, "script_path": str(cached_path)
                        }
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
            _static_safety_check(code)
        except Exception as exc:
            local_reflection = (
                f"Your code was rejected by the static safety screen: {exc}. "
                "Do not use OS/process/network client modules, direct file-opening builtins, "
                "eval/exec/introspection, or private/dunder attributes."
            )
            logger.warning("Attempt %d failed static safety screen: %s", attempt, exc)
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
        saved_path = saved_func_dir / brd_path.stem / saved_name
        return func, expected_function_name, {
            "origin": "gpt", "script_name": saved_name, "script_path": str(saved_path)
        }

    logger.error("❌ All %d attempts failed for %s", MAX_ATTEMPTS, brd_path.name)
    return None, None, None

# Quarantine helpers
def _reg_get(reg: dict, key: str, default):
    v = reg.get(key)
    return v if isinstance(v, type(default)) else default


def _source_content_hash(path: pathlib.Path | str | None) -> str | None:
    if not path:
        return None
    try:
        data = pathlib.Path(path).read_bytes()
    except Exception:
        return None
    return hashlib.blake2s(data, digest_size=16).hexdigest()


def is_quarantined(
    reg_path: pathlib.Path,
    hash_signature: str,
    script_name: str,
    script_path: pathlib.Path | str | None = None,
) -> bool:
    reg = load_registry(reg_path)
    meta = reg.get(hash_signature, {})
    current_content_hash = _source_content_hash(script_path)
    for item in _reg_get(meta, "quarantine", []):
        if item.get("script") != script_name:
            continue
        quarantined_hash = item.get("content_hash")
        if quarantined_hash and current_content_hash and quarantined_hash != current_content_hash:
            continue  # same filename, but the implementation was edited after quarantine
        return True
    return False


def add_quarantine(
    reg_path: pathlib.Path,
    hash_signature: str,
    script_name: str,
    origin: str,
    reason: str,
    registry_lock=None,
    script_path: pathlib.Path | str | None = None,
) -> None:
    content_hash = _source_content_hash(script_path)

    def _update_registry():
        reg = load_registry(reg_path)
        meta = reg.setdefault(hash_signature, {})
        q = _reg_get(meta, "quarantine", [])
        if any(
            it.get("script") == script_name
            and (not content_hash or not it.get("content_hash") or it.get("content_hash") == content_hash)
            for it in q
        ):
            return
        item = {
            "script": script_name,
            "origin": origin,
            "reason": (reason or "")[:400],
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        if content_hash:
            item["content_hash"] = content_hash
        q.append(item)
        meta["quarantine"] = q
        save_registry(reg_path, reg)

    if registry_lock is None:
        _update_registry()
    else:
        with registry_lock:
            _update_registry()



###############################   Main Agent function ######################
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

    After repeated generation failures or repeated live-batch failures for one BRD
    hash, enter a blocked state and wait for the BRD to change.
    """
    import signal, os, psutil
    import logging
    import logging.handlers

    # ---------- local constants (no dependency on job_manager.py) ----------
    WORKER_MAX_RECORD_BYTES = 64_000
    hb_max_age = HB_MAX_AGE
    hb_misses_before_exit = HB_MISSES_BEFORE_EXIT
    warn_bad_BRD_every_x_seconds = 20  # only used later; keep here for readability

    # Make only the narrow GPT wrapper available to generated code.
    global _active_gpt_semaphore
    _active_gpt_semaphore = gpt_semaphore
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
    cached_id_field = None
    cached_expected_field = None
    last_brd_hash = None
    last_reflection = None
    last_impl_meta = None

    # Blocked state: after repeated failures for a specific BRD hash
    blocked_hash: str | None = None
    blocked_reason: str | None = None
    next_block_warn_ts: float = 0.0  # throttle warnings to 20s
    schema_fail_count = 0  # consecutive schema-extraction failures -> exponential backoff
    batch_fail_count = 0
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
                    batch_fail_count = 0
                    worker_logger.info("BRD content changed — invalidating cache for %s", brd_path.name)
                    cached_func = None
                    cached_schema = None
                    cached_contract = None
                    cached_brd_tests = None
                    cached_n_items_to_pass = None
                    cached_id_field = None
                    cached_expected_field = None
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
                if (
                    cached_contract is None
                    or cached_brd_tests is None
                    or cached_n_items_to_pass is None
                    or cached_id_field is None
                    or cached_expected_field is None
                ):
                    try:
                        contract = extract_brd_contract(brd_text)
                        brd_tests = extract_brd_tests(brd_text, contract)
                        job_directives = extract_job_directives(brd_text)
                        n_items_to_pass = job_directives["n_items_to_pass"]
                        id_field = job_directives["id_field"]
                        expected_field = job_directives["expected_field"]
                        cached_contract = contract
                        cached_brd_tests = brd_tests
                        cached_n_items_to_pass = n_items_to_pass
                        cached_id_field = id_field
                        cached_expected_field = expected_field
                        module_logger.info(
                            "Parsed BRD inspector data for %s: inputs=%s tests=%d ID_FIELD=%s EXPECTED_FIELD=%s N_ITEMS_TO_PASS=%d",
                            brd_path.name, [item["name"] for item in contract["input"]],
                            len(brd_tests), id_field, expected_field, n_items_to_pass,
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
                    id_field = cached_id_field
                    expected_field = cached_expected_field

                # ----------------- Function name (LLM) + Python-owned job metadata -----------------
                if cached_schema is None:
                    try:
                        schema = gpt_extract_function_and_data_schema(brd_text, gpt_semaphore)
                        schema["inputs"] = [item["name"] for item in contract["input"]]
                        schema["incident_id"] = id_field
                        schema["expected"] = expected_field
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
                    schema["incident_id"] = id_field
                    schema["expected"] = expected_field

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
                            rejected_hashes = _read_rejected_hashes(done_file)
                            new_jobs = []
                            for rec in jobs:
                                if _runtime_record_hash(rec) in rejected_hashes:
                                    continue
                                if isinstance(rec, dict) and id_field in rec and str(rec[id_field]) in done_ids:
                                    continue
                                new_jobs.append(rec)
                            has_new_jobs = bool(new_jobs)

                            if not has_new_jobs and last_jobs_state != "idle":
                                module_logger.info("No actionable jobs for %s — all processed or rejected.", brd_path.name)
                                last_jobs_state = "idle"
                            elif has_new_jobs and last_jobs_state != "pending":
                                module_logger.info("%d actionable job(s) for %s.",
                                                   len(new_jobs), brd_path.name)
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
                    input_specs=contract["input"],
                    compare_expected=True,
                )
                if ok_batch:
                    worker_logger.info("✅ Jobs processed for %s", brd_path.name)
                    last_reflection = None
                    batch_fail_count = 0
                    last_activity_time = now
                    time.sleep(check_interval)
                else:
                    worker_logger.warning("❌ Batch failed for %s: %s", brd_path.name, reflection)
                    try:
                        if last_impl_meta and last_brd_hash == current_hash:
                            add_quarantine(
                                reg_path=registry_path,
                                hash_signature=current_hash,
                                script_name=last_impl_meta.get("script_name", "<unknown>"),
                                origin=last_impl_meta.get("origin", "unknown"),
                                reason=(reflection or "batch failed"),
                                registry_lock=registry_lock,
                                script_path=last_impl_meta.get("script_path"),
                            )
                            module_logger.info(
                                "Quarantined %s (%s) for BRD hash %s",
                                last_impl_meta.get("script_name"), last_impl_meta.get("origin"), current_hash,
                            )
                    except Exception as q_exc:
                        module_logger.warning("Failed to quarantine impl: %s", q_exc)

                    last_reflection = reflection
                    cached_func = None
                    last_impl_meta = None
                    batch_fail_count += 1
                    if batch_fail_count >= MAX_BATCH_FAILURES:
                        blocked_hash = current_hash
                        blocked_reason = (
                            f"{batch_fail_count} consecutive live-batch failures; "
                            f"last: {(reflection or '')[:200]}"
                        )
                        next_block_warn_ts = 0.0
                        module_logger.warning("Entering blocked mode for %s: %s", brd_path.name, blocked_reason)
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
                cached_id_field = None
                cached_expected_field = None
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