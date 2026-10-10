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
from openai import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError
from dotenv import load_dotenv
from openpyxl import load_workbook
from typing import Callable, Any, List, Set, Tuple, Dict, Optional
import traceback
import tempfile
import ast
import logging, sys
import uuid

# Load a developer-local .env before reading any configured timeout.
# Existing shell/environment variables take precedence.
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
load_dotenv(_PROJECT_ROOT / ".env", override=False)

_RETRY_STEPS = 6
# Limits for GPT as an external service; independent of function-compute watchdog.
GPT_TOOL_TIMEOUT_SECONDS = max(1.0, float(os.getenv("GPT_TOOL_TIMEOUT_SECONDS", "180")))
GPT_REQUEST_TIMEOUT_SECONDS = max(1.0, float(os.getenv("GPT_REQUEST_TIMEOUT_SECONDS", "60")))
# Generation may require longer than the GPT tool used by active functions.
# The generation request gets 5 minutes, with a 10-minute overall retry budget.
GPT_GENERATION_REQUEST_TIMEOUT_SECONDS = max(1.0, float(os.getenv("GPT_GENERATION_REQUEST_TIMEOUT_SECONDS", "300")))
GPT_GENERATION_TOOL_TIMEOUT_SECONDS = max(1.0, float(os.getenv("GPT_GENERATION_TOOL_TIMEOUT_SECONDS", "600")))
GPT_SEMAPHORE_WAIT_TIMEOUT_SECONDS = max(1.0, float(os.getenv("GPT_SEMAPHORE_WAIT_TIMEOUT_SECONDS", "60")))
MAX_ATTEMPTS = 10
# Maximum actionable records a BRD worker processes before yielding for fairness.
MAX_JOBS_PER_WORKER_SESSION = max(1, int(os.getenv("MAX_JOBS_PER_WORKER_SESSION", "25")))
# 0 disables the per-invocation GPT request count cap (not recommended).
MAX_GPT_CALLS_PER_FUNCTION = max(0, int(os.getenv("MAX_GPT_CALLS_PER_FUNCTION", "5")))
# Local final-result validation uses the same limits as the manager watchdog.
FUNCTION_CALL_TIMEOUT_SECONDS = float(os.getenv("FUNCTION_CALL_TIMEOUT_SECONDS", "120"))
FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS = float(os.getenv("FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS", "300"))
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
        max_retries=0,  # all backoff is handled by gpt_call_with_retry
    )

_active_gpt_semaphore = None

# Trusted, narrow capabilities for generated functions.
# Generated code still cannot use open(), import openpyxl, or browse arbitrary paths.
_DOCUMENTS_ROOT = (_PROJECT_ROOT / "documents").resolve()


def _resolve_trusted_excel_path(relative_path: str) -> pathlib.Path:
    """Resolve an Excel workbook path while confining access to documents/."""
    if not isinstance(relative_path, str) or not relative_path.strip():
        raise ValueError("Excel path must be a non-empty relative string")

    requested = pathlib.Path(relative_path)
    if requested.is_absolute():
        raise ValueError("Excel path must be relative to the project root")

    candidate = _PROJECT_ROOT / requested
    normalized = candidate.resolve(strict=False)
    if _DOCUMENTS_ROOT not in normalized.parents:
        raise ValueError("Generated code may read Excel files only under documents/")

    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError:
        raise FileNotFoundError(f"Excel workbook not found: {relative_path}") from None

    # Re-check after strict resolution so a symlink inside documents/ cannot escape it.
    if _DOCUMENTS_ROOT not in resolved.parents:
        raise ValueError("Generated code may read Excel files only under documents/")

    if resolved.suffix.lower() not in {".xlsx", ".xlsm"}:
        raise ValueError("Trusted Excel access supports only .xlsx and .xlsm files")

    if not resolved.is_file():
        raise ValueError(f"Excel path is not a file: {relative_path}")

    return resolved


def _trusted_read_excel_rows(relative_path: str, sheet_name: str) -> list[list[Any]]:
    """Read one worksheet as values-only rows from an approved documents/ workbook."""
    if not isinstance(sheet_name, str) or not sheet_name.strip():
        raise ValueError("sheet_name must be a non-empty string")

    path = _resolve_trusted_excel_path(relative_path)
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        if sheet_name not in workbook.sheetnames:
            raise ValueError(
                f"Worksheet {sheet_name!r} not found in {relative_path!r}; "
                f"available sheets: {workbook.sheetnames}"
            )
        worksheet = workbook[sheet_name]
        return [list(row) for row in worksheet.iter_rows(values_only=True)]
    finally:
        workbook.close()


def _trusted_excel_modified_time(relative_path: str) -> float:
    """Return mtime for an approved documents/ Excel workbook."""
    return _resolve_trusted_excel_path(relative_path).stat().st_mtime


def _trusted_monotonic_time() -> float:
    """Return a monotonic clock value for safe cache-age calculations."""
    return time.monotonic()


# Synthetic module exposed to generated functions. Keep this surface deliberately small.
import types
toolbox = types.ModuleType("my_tools")
toolbox.log = logging.getLogger("my_tools")
toolbox.gpt_model = AZURE_OPENAI_DEPLOYMENT
toolbox.ask_gpt = None
toolbox.read_excel_rows = _trusted_read_excel_rows
toolbox.file_modified_time = _trusted_excel_modified_time
toolbox.monotonic_time = _trusted_monotonic_time
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



_ID_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")   # valid Python identifier
def brd_to_function_name(brd_path: pathlib.Path) -> str:
    """Derive the implementation function name deterministically from the BRD filename.

    Example:
        BRD_word_count.txt -> word_count
        BRD_email_router.txt -> email_router
    """
    stem = brd_path.stem
    if not stem.startswith("BRD_"):
        raise ValueError(f"{brd_path.name}: expected file name to start with 'BRD_'")

    function_name = stem[len("BRD_"):]
    if not function_name or not _ID_RE.fullmatch(function_name):
        raise ValueError(
            f"{brd_path.name}: derived function name {function_name!r} is not a valid Python identifier. "
            "Rename the BRD using BRD_<valid_python_identifier>.txt."
        )
    return function_name


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
Only import from these modules: {', '.join(sorted(_ALLOWED_IMPORT_ROOTS - {'my_tools'}))}.
Do not use eval/exec/compile, direct file access, getattr/introspection, frame objects,
or private/dunder attributes. Use f-strings, not str.format().
Do not import openpyxl or use open(). If the BRD requires an Excel workbook under
the project's documents/ directory, use only these trusted capabilities:
- my_tools.read_excel_rows(relative_path, sheet_name) -> list of values-only rows.
  It is read-only and always uses data_only=True.
- my_tools.file_modified_time(relative_path) -> workbook modification time.
- my_tools.monotonic_time() -> monotonic clock value for cache-age checks.
These wrappers reject paths outside documents/ and non-.xlsx/.xlsm files.
If the BRD requires an LLM call, use my_tools.ask_gpt(messages, temperature=...).
Never access a raw authenticated model client, shared_data, or a semaphore directly.
"""

    user_prompt = ""
    if reflection:
        user_prompt += reflection + "\n\n"
    user_prompt += "Requirement:\n" + brd_text

    resp = gpt_call_with_retry(
        gpt_semaphore,
        request_timeout_seconds=GPT_GENERATION_REQUEST_TIMEOUT_SECONDS,
        tool_timeout_seconds=GPT_GENERATION_TOOL_TIMEOUT_SECONDS,
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


# Allowlist: generated code may import only these modules. This is a static
# defense-in-depth screen, not a sandbox.
_ALLOWED_IMPORT_ROOTS = {
    "re", "math", "string", "json", "collections", "itertools", "functools",
    "datetime", "decimal", "fractions", "statistics", "unicodedata", "typing",
    "heapq", "bisect", "copy", "enum", "textwrap", "difflib", "random",
    "my_tools",
}
_FORBIDDEN_NAMES = {
    "exec", "eval", "compile", "open", "__import__", "globals", "locals", "vars",
    "getattr", "setattr", "delattr", "breakpoint", "input", "help", "exit", "quit",
    "__builtins__",
}
_FORBIDDEN_ATTRS = {
    "gpt_client", "shared_data", "gpt_semaphore",
    "format", "format_map", "vformat", "Formatter",
    "get_type_hints", "ForwardRef",
    "gi_frame", "gi_code", "cr_frame", "cr_code", "ag_frame", "ag_code",
    "f_globals", "f_locals", "f_builtins", "f_back", "f_code",
    "tb_frame", "tb_next", "co_code", "co_consts",
}


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
                if alias.name.split(".")[0] not in _ALLOWED_IMPORT_ROOTS:
                    raise ValueError(f"line {node.lineno}: import of {alias.name!r} is not allowed")
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".")[0]
            if node.level or root not in _ALLOWED_IMPORT_ROOTS:
                raise ValueError(f"line {node.lineno}: import from {node.module!r} is not allowed")
            for alias in node.names:
                if alias.name == "*" or alias.name in _FORBIDDEN_ATTRS or alias.name in _FORBIDDEN_NAMES:
                    raise ValueError(f"line {node.lineno}: importing {alias.name!r} is not allowed")
        elif isinstance(node, ast.Name) and node.id in _FORBIDDEN_NAMES:
            raise ValueError(f"line {node.lineno}: use of {node.id!r} is not allowed")
        elif isinstance(node, ast.Attribute):
            if node.attr.startswith("_"):
                raise ValueError(f"line {node.lineno}: private/dunder attribute {node.attr!r} is not allowed")
            if node.attr in _FORBIDDEN_ATTRS:
                raise ValueError(f"line {node.lineno}: attribute {node.attr!r} is not allowed in generated code")



def _extract_section_body(brd_text: str, heading: str) -> str:
    """Return a rigid BRD section body by matching a true underlined heading."""
    lines = brd_text.splitlines()
    wanted = heading.strip().upper()

    def _normalized_heading(line: str) -> str:
        text = line.strip()
        match = re.fullmatch(r"\d+\.\s+(.+)", text)
        if match:
            text = match.group(1).strip()
        return text.rstrip(":").strip().upper()

    def _has_heading_underline(index: int) -> bool:
        next_index = index + 1
        while next_index < len(lines) and not lines[next_index].strip():
            next_index += 1
        return next_index < len(lines) and bool(re.fullmatch(r"[-=]{3,}", lines[next_index].strip()))

    def _is_numbered_section_heading(index: int) -> bool:
        return bool(re.fullmatch(r"\d+\.\s+\S.*", lines[index].strip())) and _has_heading_underline(index)

    start = None
    for i, line in enumerate(lines):
        if _normalized_heading(line) == wanted and _has_heading_underline(i):
            start = i + 1
            break
    if start is None:
        raise ValueError(f"BRD is missing required underlined section: {heading}")

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
    exp_field = jobs_schema["expected"]
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
    exp_field = jobs_schema["expected"]

    for ordinal, rec in enumerate(deployment_jobs, start=1):
        incident_id = str(rec.get(id_field, "?")) if id_field else "?"
        params = [rec[name] for name in param_names]
        expected = rec[exp_field]

        try:
            result = _invoke_monitored(func, *params, phase="deployment_validation")
        except (ExternalGPTUnavailable, GPTServiceConfigurationError):
            raise
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
            got = _invoke_monitored(func, *args, phase="brd_example")
        except (ExternalGPTUnavailable, GPTServiceConfigurationError):
            raise
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

def get_brd_runtime_block(reg_path: pathlib.Path, hash_signature: str) -> dict | None:
    """Return only BRD/function-related persistent blocks.

    Earlier versions mistakenly persisted GPT configuration errors by BRD hash.
    Ignore those legacy blocks: fixing shared credentials/deployment settings and
    restarting must not require editing every unrelated BRD. The old metadata is
    left in the registry for audit, not treated as an active block.
    """
    entry = load_registry(reg_path).get(hash_signature, {})
    block = entry.get("runtime_block") if isinstance(entry, dict) else None
    if not isinstance(block, dict) or block.get("origin") == "gpt_service_configuration":
        return None
    return block


def get_approved_handcrafted_recovery(
    reg_path: pathlib.Path,
    hash_signature: str,
    saved_func_dir: pathlib.Path,
    brd_path: pathlib.Path,
) -> dict | None:
    """A human approval is valid only for this BRD hash and exact .py bytes."""
    entry = load_registry(reg_path).get(hash_signature, {})
    if not isinstance(entry, dict) or not get_brd_runtime_block(reg_path, hash_signature):
        return None
    approval = entry.get("handcrafted_recovery")
    if not isinstance(approval, dict) or approval.get("brd_hash") != hash_signature:
        return None
    path = saved_func_dir / f"{brd_path.stem}_handcrafted" / str(approval.get("script", ""))
    if path.suffix != ".py" or path.name != approval.get("script"):
        return None
    if _source_content_hash(path) != approval.get("content_hash"):
        return None
    return approval


def approve_handcrafted_recovery(
    brd_path: pathlib.Path, saved_func_dir: pathlib.Path, reg_path: pathlib.Path,
) -> dict:
    """Human-only CLI action: authorize ONE exact handwritten script for a blocked BRD.

    This never unblocks the BRD or skips validation. The worker tests the approved
    script against BRD examples AND the N real jobs before removing the block.
    Stop the manager before invoking this CLI, so registry edits are serialized.
    """
    if not brd_path.is_file() or not brd_path.name.startswith("BRD_") or brd_path.suffix != ".txt":
        raise ValueError("Provide a current BRD_*.txt file")
    signature = brd_hash_signature(brd_path.read_text(encoding="utf-8"))
    if not get_brd_runtime_block(reg_path, signature):
        raise ValueError(f"BRD {brd_path.name} is not currently blocked for human review")
    directory = saved_func_dir / f"{brd_path.stem}_handcrafted"
    scripts = sorted(directory.glob("*.py"))
    if len(scripts) != 1:
        raise ValueError(f"Expected exactly one handwritten .py in {directory}; found {len(scripts)}")
    script = scripts[0]
    content_hash = _source_content_hash(script)
    if content_hash is None:
        raise ValueError("Cannot read handwritten source")
    approval = {
        "brd_hash": signature, "script": script.name, "content_hash": content_hash,
        "approved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    reg = load_registry(reg_path)
    reg[signature]["handcrafted_recovery"] = approval
    save_registry(reg_path, reg)
    return approval


def finish_handcrafted_recovery(
    reg_path: pathlib.Path, hash_signature: str, saved_func_dir: pathlib.Path,
    brd_path: pathlib.Path, *, successful: bool, registry_lock=None,
) -> None:
    """Consume one approval; remove the persistent block ONLY after both gates pass."""
    def _finish():
        registry = load_registry(reg_path)
        meta = registry.get(hash_signature, {})
        approval = meta.get("handcrafted_recovery") if isinstance(meta, dict) else None
        if not isinstance(approval, dict):
            raise ValueError("Handcrafted recovery approval was revoked")
        # For successful recovery recheck the file's content hash to prevent a
        # silent source edit between approval, tests and activation.
        path = saved_func_dir / f"{brd_path.stem}_handcrafted" / approval["script"]
        if successful and _source_content_hash(path) != approval["content_hash"]:
            raise ValueError("Handcrafted code changed since approval; human reapproval required")
        meta.pop("handcrafted_recovery", None)
        meta.setdefault("handcrafted_recovery_history", []).append({
            "script": approval["script"], "content_hash": approval["content_hash"],
            "status": "passed" if successful else "failed",
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        if successful:
            # Approved recovery passed both gates and its source still matches approval.
            meta["handcrafted_source"] = {"script": approval["script"], "content_hash": approval["content_hash"]}
            meta.pop("runtime_block", None)
            meta["quarantine"] = [
                item for item in meta.get("quarantine", [])
                if not (item.get("script") == approval["script"]
                        and item.get("origin") == "handcrafted")
            ]
        save_registry(reg_path, registry)
    if registry_lock is None:
        _finish()
    else:
        with registry_lock:
            _finish()


def block_brd_until_changed(
    reg_path: pathlib.Path,
    hash_signature: str,
    brd_filename: str,
    reason: str,
    *,
    registry_lock=None,
    origin: str = "runtime",
) -> None:
    """Persist escalation; a process restart cannot re-enable the same BRD hash."""
    def _write():
        registry = load_registry(reg_path)
        entry = registry.setdefault(hash_signature, {})
        entry.setdefault("title", brd_filename)
        entry.setdefault("folder_name", pathlib.Path(brd_filename).stem)
        existing_block = entry.get("runtime_block")
        if not isinstance(existing_block, dict) or existing_block.get("origin") == "gpt_service_configuration":
            # Replace any obsolete configuration-only block with a real BRD or
            # function failure. Otherwise the old entry would mask the new block.
            entry["runtime_block"] = {
                "reason": str(reason)[:500],
                "origin": origin,
                "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            save_registry(reg_path, registry)

    if registry_lock is None:
        _write()
    else:
        with registry_lock:
            _write()


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
            "source_hash": hashlib.blake2s(code.encode("utf-8"), digest_size=16).hexdigest(),
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


class ExternalGPTUnavailable(RuntimeError):
    """Transient approved GPT service failure; retry without blaming the function."""


class GPTToolRequestRejected(ValueError):
    """Malformed approved GPT-tool request, potentially fixable in BRD examples."""


class GPTServiceConfigurationError(RuntimeError):
    """Credentials, deployment, access policy, or service setup requires a human."""


def _classify_gpt_error(exc: BaseException) -> str:
    """One of 'transient', 'request', or 'configuration'. Never default to retry."""
    if isinstance(exc, ExternalGPTUnavailable):
        return "transient"
    if isinstance(exc, GPTToolRequestRejected):
        return "request"
    if isinstance(exc, GPTServiceConfigurationError):
        return "configuration"
    if isinstance(exc, (RateLimitError, APIConnectionError, APITimeoutError, TimeoutError)):
        return "transient"
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        if status == 429 or status >= 500:
            return "transient"
        if status in (401, 403, 404):
            return "configuration"
        if status == 400 and any(
            marker in str(exc).lower()
            for marker in ("content_filter", "content filter", "policy_violation")
        ):
            return "configuration"
        if 400 <= status < 500:
            return "request"
    if isinstance(exc, (TypeError, ValueError)):
        return "request"
    # Unexpected SDK failures are not automatically recoverable outages.
    return "configuration"


_temperature_unsupported = False


def gpt_call_with_retry(
    semaphore, *, on_slot_change=None, check_cancel=None,
    request_timeout_seconds=None, tool_timeout_seconds=None, **kwargs,
):
    """Bound a GPT interaction separately from the generated-function watchdog.

    The API call has a per-attempt timeout, the semaphore wait is bounded,
    and the total request/retry budget is bounded. The semaphore is held
    ONLY for an API attempt, never during backoff or waiting for a slot.
    """
    global _temperature_unsupported
    if gpt_client is None or not my_tools.gpt_model:
        raise GPTServiceConfigurationError(
            "Azure OpenAI is not configured. Set AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, "
            "and AZURE_OPENAI_DEPLOYMENT. AZURE_OPENAI_API_VERSION is optional."
        )

    call_kwargs = dict(kwargs)
    if _temperature_unsupported:
        call_kwargs.pop("temperature", None)
    # Explicit generation-specific budgets do not affect GPT calls inside functions.
    request_budget = GPT_REQUEST_TIMEOUT_SECONDS if request_timeout_seconds is None else max(1.0, float(request_timeout_seconds))
    tool_budget = GPT_TOOL_TIMEOUT_SECONDS if tool_timeout_seconds is None else max(1.0, float(tool_timeout_seconds))
    deadline = time.monotonic() + tool_budget
    backoff = 1.0
    last_error = None
    for _ in range(_RETRY_STEPS):
        if check_cancel is not None:
            check_cancel()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wait = min(GPT_SEMAPHORE_WAIT_TIMEOUT_SECONDS, remaining)
        acquired = semaphore.acquire(timeout=wait)
        if not acquired:
            last_error = TimeoutError("No free GPT concurrency slot within the configured wait")
            break
        try:
            if on_slot_change is not None:
                on_slot_change(True)
            if check_cancel is not None:
                check_cancel()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                last_error = TimeoutError("GPT tool timeout exceeded before API request")
                break
            per_request = min(request_budget, remaining)
            return gpt_client.chat.completions.create(
                model=my_tools.gpt_model,
                timeout=per_request,
                **call_kwargs,
            )
        except FunctionExecutionLimitExceeded:
            raise
        except (RateLimitError, APIStatusError) as exc:
            last_error = exc
            status = getattr(exc, "status_code", None)
            message = str(exc).lower()
            if status == 400 and "temperature" in call_kwargs and "temperature" in message:
                call_kwargs.pop("temperature", None)
                _temperature_unsupported = True
                LOG.warning("Deployment rejected temperature; retrying without it (remembered for this worker).")
                continue
            if status != 429 and not (isinstance(status, int) and status >= 500):
                LOG.exception("GPT permanently rejected the request")
                raise
            LOG.warning("GPT rate-limited or server error (%s) - backing off", status)
        except (APIConnectionError, APITimeoutError) as exc:
            last_error = exc
            LOG.warning("GPT transport/timeout error (%s) - backing off", type(exc).__name__)
        finally:
            # Even on errors and cancellations, always return this permit.
            try:
                semaphore.release()
            finally:
                if on_slot_change is not None:
                    on_slot_change(False)

        if check_cancel is not None:
            check_cancel()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        sleep = min(backoff + random.uniform(0, 0.3), remaining)
        LOG.warning("GPT retry in %.1fs", sleep)
        time.sleep(sleep)
        backoff = min(backoff * 2, 60)
    raise TimeoutError(
        f"GPT service unavailable within {tool_budget:g}s or {_RETRY_STEPS} retries"
    ) from last_error


# Track all GPT tool failures within the current monitored function call.
# A generated function cannot hide an outage by catching its exception and
# returning a fabricated "successful" fallback answer.
_gpt_tool_issue_during_call: tuple[str, str] | None = None
_gpt_calls_in_monitored_call = 0
_function_limit_issue_during_call: str | None = None


class FunctionExecutionLimitExceeded(RuntimeError):
    """A generated function exceeded its invocation-level call/time budget."""


def _generated_ask_gpt(messages, temperature=0):
    """Approved GPT wrapper: enforce cost limits and honor parent cancellation."""
    global _gpt_tool_issue_during_call, _gpt_calls_in_monitored_call
    global _function_limit_issue_during_call
    mark, phase = _active_function_watchdog, _monitored_phase

    def check_cancel():
        global _function_limit_issue_during_call
        reason = mark("check_cancel", phase) if mark is not None and phase is not None else None
        if reason:
            _function_limit_issue_during_call = str(reason)
            raise FunctionExecutionLimitExceeded(str(reason))

    def slot_change(held: bool):
        if mark is not None and phase is not None:
            mark("slot_acquired" if held else "slot_released", phase)

    # Check before any remote request. The count is for one *function invocation*,
    # not for code generation or the lifetime of this worker.
    if phase is not None:
        check_cancel()
        _gpt_calls_in_monitored_call += 1
        if MAX_GPT_CALLS_PER_FUNCTION and _gpt_calls_in_monitored_call > MAX_GPT_CALLS_PER_FUNCTION:
            reason = (
                f"Function attempted more than {MAX_GPT_CALLS_PER_FUNCTION} "
                "GPT tool calls in one invocation"
            )
            _function_limit_issue_during_call = reason
            raise FunctionExecutionLimitExceeded(reason)

    if mark is not None and phase is not None:
        mark("pause", phase)
    try:
        try:
            if _active_gpt_semaphore is None:
                raise GPTServiceConfigurationError(
                    "Generated-code GPT tool is unavailable outside an active BRD worker"
                )
            if not isinstance(messages, list) or not messages or not all(
                isinstance(item, dict) and isinstance(item.get("role"), str)
                and "content" in item for item in messages
            ):
                raise GPTToolRequestRejected(
                    "my_tools.ask_gpt(messages): messages must be a nonempty list "
                    "of role/content dictionaries, not a string or arbitrary object"
                )
            kwargs = {"messages": messages}
            if temperature is not None:
                kwargs["temperature"] = temperature
            response = gpt_call_with_retry(
                _active_gpt_semaphore,
                on_slot_change=slot_change if phase is not None else None,
                check_cancel=check_cancel if phase is not None else None,
                **kwargs,
            )
            check_cancel()  # Never accept a result after a parent cancellation.
            return response.choices[0].message.content
        except FunctionExecutionLimitExceeded:
            raise  # A function-limit breach is neither GPT outage nor configuration.
        except Exception as exc:
            classification = _classify_gpt_error(exc)
            detail = f"{type(exc).__name__}: {exc}"
            if phase is not None:
                _gpt_tool_issue_during_call = (classification, detail[:500])
            if classification == "transient":
                raise ExternalGPTUnavailable(f"GPT temporarily unavailable: {detail}") from exc
            if classification == "request":
                raise GPTToolRequestRejected(f"my_tools.ask_gpt rejected request: {detail}") from exc
            raise GPTServiceConfigurationError(
                f"GPT configuration/access requires human review: {detail}"
            ) from exc
    finally:
        if mark is not None and phase is not None:
            mark("resume", phase)


toolbox.ask_gpt = _generated_ask_gpt



def _read_done_status(done_file: pathlib.Path) -> tuple[set[str], set[str]]:
    """Read worker state once, failing closed on corruption or filesystem errors."""
    try:
        data = json.loads(done_file.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return set(), set()
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DoneStatePersistenceError(f"Cannot read done state {done_file.name}: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("ids", []), list) or not isinstance(data.get("rejected", {}), dict):
        raise DoneStatePersistenceError(f"Invalid done-state structure in {done_file.name}")
    return set(map(str, data.get("ids", []))), set(data.get("rejected", {}))


def _runtime_record_hash(record: Any) -> str:
    payload = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2s(payload.encode("utf-8"), digest_size=16).hexdigest()


class JobDataTemporarilyUnavailable(RuntimeError):
    """Jobs data was unavailable or incomplete; not a function defect."""


class DoneStatePersistenceError(RuntimeError):
    """Done-state is unreadable or cannot be committed; never blame the function."""


def _invoke_monitored(func, *args, phase="function_call", **kwargs):
    """Mark only generated-function execution for the manager watchdog.

    This is a recovery timeout, not a filesystem/security sandbox. The watchdog
    may kill this worker while code runs. It is intentionally *not* set while
    Python waits for the code-generating GPT API.
    """
    global _monitored_phase, _gpt_tool_issue_during_call
    global _gpt_calls_in_monitored_call, _function_limit_issue_during_call
    mark = _active_function_watchdog
    previous_phase = _monitored_phase
    previous_issue = _gpt_tool_issue_during_call
    previous_count = _gpt_calls_in_monitored_call
    previous_limit_issue = _function_limit_issue_during_call
    wall_started_locally = time.monotonic()

    def _check_completion_budget() -> None:
        """Validate even when a function finishes between supervisor polls."""
        global _function_limit_issue_during_call
        if mark is not None:
            reason = mark("check_budget", phase)
            if reason:
                _function_limit_issue_during_call = str(reason)
        # A local wall-clock fallback also works if a shared marker was lost.
        if (FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS > 0
                and time.monotonic() - wall_started_locally > FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS):
            _function_limit_issue_during_call = (
                f"Function exceeded total_wall budget before return "
                f"(limit {FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS:g}s)"
            )
        if _function_limit_issue_during_call:
            raise FunctionExecutionLimitExceeded(_function_limit_issue_during_call)

    if mark is not None:
        mark("start", phase)
    _monitored_phase = phase
    _gpt_tool_issue_during_call = None
    _gpt_calls_in_monitored_call = 0
    _function_limit_issue_during_call = None
    try:
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            try:
                _check_completion_budget()
            except FunctionExecutionLimitExceeded as budget_exc:
                raise budget_exc from exc
            # A generated function may have caught a tool error, then failed
            # for another reason. Preserve the true tool-failure classification.
            if _gpt_tool_issue_during_call is not None:
                kind, detail = _gpt_tool_issue_during_call
                if kind == "transient":
                    raise ExternalGPTUnavailable(detail) from exc
                if kind == "configuration":
                    raise GPTServiceConfigurationError(detail) from exc
            raise
        _check_completion_budget()
        if mark is not None:
            cancelled = mark("check_cancel", phase)
            if cancelled:
                _function_limit_issue_during_call = str(cancelled)
        if _function_limit_issue_during_call:
            raise FunctionExecutionLimitExceeded(_function_limit_issue_during_call)
        if _gpt_tool_issue_during_call is not None:
            kind, detail = _gpt_tool_issue_during_call
            if kind == "transient":
                raise ExternalGPTUnavailable(
                    f"GPT outage during function call; output discarded: {detail}"
                )
            if kind == "configuration":
                raise GPTServiceConfigurationError(
                    f"GPT configuration problem during function call; output discarded: {detail}"
                )
            raise GPTToolRequestRejected(
                f"Generated function suppressed an invalid GPT request; output discarded: {detail}"
            )
        return result
    finally:
        _gpt_tool_issue_during_call = previous_issue
        _gpt_calls_in_monitored_call = previous_count
        _function_limit_issue_during_call = previous_limit_issue
        _monitored_phase = previous_phase
        if mark is not None:
            mark("end", phase)


_active_function_watchdog = None
_monitored_phase = None


def process_jobs(
    func: Callable[..., Any],
    jobs_schema: Dict[str, Any],
    jobs_path: pathlib.Path,
    done_file: Optional[pathlib.Path] = None,
    *,
    output_contract: dict[str, Any] | None = None,
    input_specs: list[dict[str, Any]] | None = None,
    preview_limit: int = 1200,
    compare_expected: bool = True,
    max_jobs: int | None = None,
) -> Tuple[bool, Optional[str]]:
    """Run runtime jobs with BRD input/output validation and durable progress state."""
    if not jobs_path.exists():
        raise JobDataTemporarilyUnavailable(
            f"Jobs file {jobs_path.name} is temporarily missing; will retry"
        )

    try:
        jobs = json.loads(jobs_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        # Upstream writers may be updating the file: this is a DATA problem.
        # Do not misclassify it as a generated-function failure.
        raise JobDataTemporarilyUnavailable(
            f"Jobs file {jobs_path.name} unavailable/incomplete: {type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(jobs, list):
        raise JobDataTemporarilyUnavailable(
            f"Jobs file {jobs_path.name} is not a JSON list of job records"
        )
    if not jobs:
        LOG.info("Empty jobs file %s - nothing to do.", jobs_path.name)
        return True, None

    param_names: List[str] = list(jobs_schema.get("inputs") or [])
    id_field: Optional[str] = jobs_schema.get("incident_id")
    exp_field: Optional[str] = jobs_schema["expected"]
    if not id_field:
        raise ValueError("Runtime jobs require a Python-parsed ID_FIELD")

    if done_file is None:
        done_file = jobs_path.parent / f"done_{jobs_path.stem}.json"
    try:
        done_file.parent.mkdir(parents=True, exist_ok=True)
        raw_done = json.loads(done_file.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raw_done = {"ids": [], "rejected": {}}
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DoneStatePersistenceError(f"Cannot read done state {done_file.name}: {exc}") from exc
    if not isinstance(raw_done, dict) or not isinstance(raw_done.get("ids", []), list) or not isinstance(raw_done.get("rejected", {}), dict):
        raise DoneStatePersistenceError(f"Invalid done-state structure in {done_file.name}")
    done_ids: Set[str] = set(map(str, raw_done.get("ids", [])))
    rejected: Dict[str, Any] = dict(raw_done.get("rejected", {}))

    def _persist_done() -> None:
        # Never mark a job completed unless its state was durably written.
        try:
            _atomic_write_json(done_file, {"ids": sorted(done_ids), "rejected": rejected})
        except Exception as exc:
            raise DoneStatePersistenceError(f"Cannot persist done state {done_file.name}: {exc}") from exc

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

    for index, rec in enumerate(new_jobs):
        if max_jobs is not None and index >= max_jobs:
            break  # Remaining records are picked up by the next scheduled worker.
        record_hash = _runtime_record_hash(rec)
        incident_id = "?"

        def _reject(reason: str) -> None:
            rejected[record_hash] = {"incident_id": incident_id, "reason": reason}
            _persist_done()
            LOG.warning("incident_id=%s rejected (data validation; function not called): %s", incident_id, reason)

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
            result = _invoke_monitored(func, *params, phase="live_job")

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
                    return False, reflection

            done_ids.add(incident_id)
            # If a corrected record for this incident succeeds, clear older rejection entries for that ID.
            rejected = {
                key: value for key, value in rejected.items()
                if not isinstance(value, dict) or value.get("incident_id") != incident_id
            }
            _persist_done()  # Write each success before logging it.
            LOG.info("incident_id=%s: %s", incident_id, result)

        except (ExternalGPTUnavailable, GPTServiceConfigurationError, DoneStatePersistenceError):
            raise
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
        
class BlindDeploymentValidationFailure(RuntimeError):
    """A generated candidate failed the blind N-job gate.

    Keep this exception's message free of real-job inputs and expected answers:
    it is for control flow / human escalation, never for GPT reflection.
    """


class DeploymentValidationDataUnavailable(RuntimeError):
    """A new or modified implementation needs labeled jobs before first use."""


def _get_or_create_function(
    brd_path: pathlib.Path,
    brd_text: str,
    hash_signature: str,
    schema: dict,
    contract: dict[str, Any],
    brd_tests: list[tuple[tuple[Any, ...], Any, int]],
    n_items_to_pass: int,
    jobs_path: pathlib.Path,
    saved_func_dir: pathlib.Path,
    registry_path: pathlib.Path,
    gpt_semaphore: multiprocessing.Semaphore,
    registry_lock,
    logger,
    is_debug_mode: bool,
    reflection: str | None = None,
    handcrafted_only: bool = False,
):
    """Load/generate implementations; an approved recovery may test ONLY handcrafted."""
    skip_reuse_once = bool(reflection)
    expected_function_name = schema["function_name"]
    param_names = [input_spec["name"] for input_spec in contract["input"]]

    jobs_schema = {
        "inputs": param_names,
        "incident_id": schema["incident_id"],
        "expected": schema["expected"],
    }
    deployment_jobs = None

    def need_deployment_jobs():
        nonlocal deployment_jobs
        if deployment_jobs is None:
            try:
                deployment_jobs, labeled_count = prepare_deployment_validation_jobs(
                    jobs_path, jobs_schema, contract, n_items_to_pass
                )
            except Exception as exc:
                raise DeploymentValidationDataUnavailable(str(exc)) from exc
            logger.info("Deployment preflight for %s: using first %d labeled job(s) out of %d labeled",
                        brd_path.name, n_items_to_pass, labeled_count)
        return deployment_jobs

    def remember_source(key: str, metadata: dict) -> None:
        # Registry metadata is keyed by the BRD hash; no separate validation flag.
        def update():
            registry = load_registry(registry_path)
            entry = registry.setdefault(hash_signature, {})
            entry.setdefault("title", brd_path.name)
            entry.setdefault("folder_name", brd_path.stem)
            entry[key] = metadata
            save_registry(registry_path, registry)
        if registry_lock is None:
            update()
        else:
            with registry_lock:
                update()

    # [1] Handcrafted implementation.
    handcrafted = saved_func_dir / f"{brd_path.stem}_handcrafted"
    if handcrafted.exists() and (handcrafted_only or not skip_reuse_once):
        logger.info("Found handcrafted dir %s", handcrafted)
        try:
            py_files = sorted(handcrafted.glob("*.py"))
            if not py_files:
                raise RuntimeError(f"No .py files in {handcrafted}")
            if len(py_files) > 1:
                raise RuntimeError(f"Expected exactly one .py file in {handcrafted}; found {len(py_files)}")
            py_path = py_files[0]
            script_name = py_path.name
            if not handcrafted_only and is_quarantined(registry_path, hash_signature, script_name, script_path=py_path):
                logger.info("Handcrafted impl %s is quarantined; skipping.", script_name)
            else:
                source_hash = _source_content_hash(py_path)
                if source_hash is None:
                    raise OSError(f"Cannot read handcrafted source: {py_path}")
                hc_mod = load_script(namespace=f"hc_{hash_signature}", py_path=py_path)
                if _source_content_hash(py_path) != source_hash:
                    raise RuntimeError("Handcrafted source changed while loading; retry required")
                if hasattr(hc_mod, expected_function_name):
                    hc_func_name = expected_function_name
                    hc_func = getattr(hc_mod, hc_func_name)
                else:
                    public_callables = [
                        (name, obj)
                        for name, obj in vars(hc_mod).items()
                        if not name.startswith("_")
                        and callable(obj)
                        and getattr(obj, "__module__", None) == hc_mod.__name__
                    ]
                    if len(public_callables) != 1:
                        raise RuntimeError(
                            f"Handcrafted module should define {expected_function_name}(), or contain exactly "
                            f"one public function defined in that module; found {[name for name, _ in public_callables]}"
                        )
                    hc_func_name, hc_func = public_callables[0]
                    logger.info(
                        "Handcrafted module uses legacy/custom function name %s(); "
                        "deterministic BRD-derived name is %s().",
                        hc_func_name, expected_function_name,
                    )
                existing = load_registry(registry_path).get(hash_signature, {})
                registered = existing.get("handcrafted_source", {}) if isinstance(existing, dict) else {}
                unchanged = (not handcrafted_only and isinstance(registered, dict)
                             and registered.get("script") == script_name
                             and registered.get("content_hash") == source_hash)
                if unchanged:
                    logger.info("✨ Reusing unchanged registered handcrafted %s() without deployment tests", hc_func_name)
                    return hc_func, hc_func_name, {
                        "origin": "handcrafted", "script_name": script_name, "script_path": str(py_path)
                    }
                validated_jobs = need_deployment_jobs()
                ok_brd, _ = run_brd_tests(hc_func, brd_tests, contract)
                if ok_brd:
                    ok_deploy, deploy_reflection = run_deployment_validation_jobs(
                        hc_func, jobs_schema, validated_jobs, contract["output"]
                    )
                    if ok_deploy:
                        if _source_content_hash(py_path) != source_hash:
                            raise RuntimeError("Handcrafted source changed during validation; retry required")
                        if not handcrafted_only:
                            remember_source("handcrafted_source", {"script": script_name, "content_hash": source_hash})
                        logger.info("✅ Using handcrafted %s() after passing %d labeled deployment job(s)",
                                    expected_function_name, n_items_to_pass)
                        return hc_func, hc_func_name, {
                            "origin": "handcrafted", "script_name": script_name, "script_path": str(py_path)
                        }
                    logger.warning("Handcrafted impl failed labeled deployment validation: %s", deploy_reflection)
                else:
                    logger.warning("Handcrafted impl failed BRD-authored tests/contract")
        except (ExternalGPTUnavailable, GPTServiceConfigurationError, DeploymentValidationDataUnavailable):
            raise
        except Exception as exc:
            logger.warning("Handcrafted load/test failed: %s", exc)
    elif handcrafted.exists() and skip_reuse_once:
        logger.info("Previous batch failure -> skipping handcrafted reuse this loop.")

    if handcrafted_only:
        logger.error("Approved handcrafted recovery failed tests; no cached or GPT fallback is allowed")
        return None, None, None

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
                source_hash = _source_content_hash(cached_path)
                if source_hash is None:
                    raise OSError(f"Cannot read cached source: {cached_path}")
                _static_safety_check(cached_path.read_text(encoding="utf-8"))
                cached_mod = _invoke_monitored(
                    load_script, phase="cached_code_load",
                    namespace=f"saved_{hash_signature}",
                    saved_func_dir=saved_func_dir,
                    hash_signature=hash_signature,
                    script_name=script_name,
                    registry_path=registry_path,
                )
                if not hasattr(cached_mod, func_name):
                    raise RuntimeError(f"Cached module missing {func_name}()")
                cached_func = getattr(cached_mod, func_name)
                if _source_content_hash(cached_path) != source_hash:
                    raise RuntimeError("Cached source changed while loading; retry required")
                registered_hash = meta.get("source_hash")
                if registered_hash is None:
                    # Older registry entries were written only after successful tests.
                    # Adopt their existing script without needing historic labeled jobs.
                    remember_source("source_hash", source_hash)
                    registered_hash = source_hash
                if source_hash == registered_hash:
                    logger.info("✨ Reusing unchanged registered %s() without deployment tests", func_name)
                    return cached_func, func_name, {
                        "origin": "cached", "script_name": script_name, "script_path": str(cached_path)
                    }
                validated_jobs = need_deployment_jobs()
                ok_brd, _ = run_brd_tests(cached_func, brd_tests, contract)
                if ok_brd:
                    ok_deploy, deploy_reflection = run_deployment_validation_jobs(
                        cached_func, jobs_schema, validated_jobs, contract["output"]
                    )
                    if ok_deploy:
                        if _source_content_hash(cached_path) != source_hash:
                            raise RuntimeError("Cached source changed during validation; retry required")
                        remember_source("source_hash", source_hash)
                        logger.info("✅ Requalified modified cached %s() after BRD + %d labeled jobs",
                                    func_name, n_items_to_pass)
                        return cached_func, func_name, {
                            "origin": "cached", "script_name": script_name, "script_path": str(cached_path)
                        }
                    logger.info("Cached impl failed labeled deployment validation: %s", deploy_reflection)
                else:
                    logger.info("Cache invalid – BRD-authored tests/contract failed")
        except (ExternalGPTUnavailable, GPTServiceConfigurationError, DeploymentValidationDataUnavailable):
            raise
        except Exception as exc:
            logger.warning("Cache load/test failed: %s", exc)
    elif latest_script and skip_reuse_once:
        logger.info("Previous batch failure -> skipping cached reuse this loop.")

    # [3] GPT generation. Never call the generator without the first N labeled jobs.
    validated_jobs = need_deployment_jobs()
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
            kind = _classify_gpt_error(exc)
            if kind == "transient":
                # Service outage does not consume code-repair attempts.
                raise ExternalGPTUnavailable(
                    f"GPT code-generation service temporarily unavailable: {type(exc).__name__}: {exc}"
                ) from exc
            if kind == "configuration":
                raise GPTServiceConfigurationError(
                    f"GPT code-generation configuration/policy error: {type(exc).__name__}: {exc}"
                ) from exc
            # A rejected generator request counts toward the ten-attempt cap.
            local_reflection = f"GPT request rejected: {type(exc).__name__}: {exc}"
            logger.error("Attempt %d: GPT generation request invalid: %s", attempt, exc)
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
                f"Only import from: {', '.join(sorted(_ALLOWED_IMPORT_ROOTS - {'my_tools'}))}. "
                "No eval/exec, direct file access, getattr/introspection, frame objects, str.format(), "
                "or private/dunder attributes. For approved Excel data under documents/, use "
                "my_tools.read_excel_rows(...), my_tools.file_modified_time(...), and "
                "my_tools.monotonic_time()."
            )
            logger.warning("Attempt %d failed static safety screen: %s", attempt, exc)
            continue

        try:
            ns = {"my_tools": my_tools}
            _invoke_monitored(exec, code, ns, phase="generated_code_load")
            func = ns.get(expected_function_name)
            if not callable(func):
                raise ValueError(f"Function {expected_function_name} not found or not callable")
            logger.info("Code compilation successful - function %s loaded", expected_function_name)
        except (ExternalGPTUnavailable, GPTServiceConfigurationError):
            raise
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
            func, jobs_schema, validated_jobs, contract["output"]
        )
        if not ok_deploy:
            # BLIND HOLDOUT: the Python validator may log the failure for a human,
            # but MUST NOT pass reflection2 (inputs/expected/actual) to GPT.
            # Unlike BRD-authored example failures, a failed real-job test does
            # not trigger another generation attempt for this BRD hash.
            logger.error(
                "Blind deployment gate failed after BRD examples passed "
                "(attempt %d, N_ITEMS_TO_PASS=%d). "
                "Rejecting candidate and escalating to a human; no real-job feedback to GPT.",
                attempt, n_items_to_pass,
            )
            raise BlindDeploymentValidationFailure(
                f"Generated candidate failed blind validation on the first "
                f"{n_items_to_pass} labeled real jobs. Human review required; "
                "no real-job data was sent to the code-generating LLM."
            )

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

    A valid live-job execution failure immediately quarantines the implementation
    and blocks this BRD hash for human review; no automatic GPT regeneration.
    Blocks are persisted across restarts in the registry.
    """
    import signal, os, psutil
    import logging
    import logging.handlers

    # ---------- local constants (no dependency on job_manager.py) ----------
    WORKER_MAX_RECORD_BYTES = 64_000
    hb_max_age = HB_MAX_AGE
    hb_misses_before_exit = HB_MISSES_BEFORE_EXIT
    warn_bad_BRD_every_x_seconds = 20  # only used later; keep here for readability

    def _yield_slot(reason: str) -> None:
        """Release execution capacity; manager reacts to future input/BRD changes."""
        try:
            shared_data["shared_dict"][f"worker_yield::{brd_path.name}"] = {
                "pid": os.getpid(), "reason": reason,
                "at": time.time(),
            }
        except (OSError, EOFError, BrokenPipeError):
            pass
        worker_logger.info("Yielding BRD worker slot: %s (%s)", brd_path.name, reason)

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
    # Each spawned BRD worker owns its own monitoring callback. Only an active
    # function call is timed; regular polling and GPT code generation are not.
    global _active_function_watchdog, _monitored_phase
    def _mark_function_call(event: str, phase: str) -> None:
        """Track cumulative computation time; pause only approved GPT-tool waits."""
        key = f"function_call::{brd_path.name}"
        try:
            shared = shared_data["shared_dict"]
            now = time.monotonic()
            if event == "start":
                shared[key] = {
                    "pid": os.getpid(),
                    "invocation_id": uuid.uuid4().hex,
                    "started_monotonic": now,
                    "wall_started_monotonic": now,  # Does not pause for GPT calls.
                    "phase": phase,
                    "paused": False,
                    "gpt_slot_held": False,
                }
                return
            state = shared.get(key)
            if not isinstance(state, dict) or state.get("pid") != os.getpid():
                return
            if event == "check_budget":
                # A worker-side check catches returns between manager polling
                # cycles, before the result can be logged or marked done.
                compute_elapsed = (
                    float(state.get("elapsed_compute", 0.0)) if state.get("paused", False)
                    else now - float(state.get("started_monotonic", now))
                )
                wall_elapsed = now - float(state.get("wall_started_monotonic", now))
                if FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS > 0 and wall_elapsed > FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS:
                    return (f"Function exceeded total_wall budget: {wall_elapsed:.2f}s "
                            f"(limit {FUNCTION_TOTAL_WALL_TIMEOUT_SECONDS:g}s)")
                if FUNCTION_CALL_TIMEOUT_SECONDS > 0 and compute_elapsed > FUNCTION_CALL_TIMEOUT_SECONDS:
                    return (f"Function exceeded computation budget: {compute_elapsed:.2f}s "
                            f"(limit {FUNCTION_CALL_TIMEOUT_SECONDS:g}s)")
                return None
            if event == "check_cancel":
                # Cancellation is stored separately by the manager. It must not
                # write the worker-owned function_call record, which includes
                # gpt_slot_held; a stale overwrite could leak a semaphore slot.
                cancel = shared.get(f"cancel::{brd_path.name}")
                if (isinstance(cancel, dict) and cancel.get("pid") == os.getpid()
                        and cancel.get("invocation_id") == state.get("invocation_id")):
                    return cancel.get("reason") or "Function execution budget exceeded"
                return None
            if event == "slot_acquired":
                state["gpt_slot_held"] = True
                shared[key] = state
            elif event == "slot_released":
                state["gpt_slot_held"] = False
                shared[key] = state
            elif event == "end":
                shared.pop(key, None)
            elif event == "pause" and not state.get("paused", False):
                state["elapsed_compute"] = max(0.0, now - float(state["started_monotonic"]))
                state["paused"] = True
                shared[key] = state
            elif event == "resume" and state.get("paused", False):
                elapsed = float(state.pop("elapsed_compute", 0.0))
                state["started_monotonic"] = now - elapsed
                state["paused"] = False
                shared[key] = state
        except (OSError, EOFError, BrokenPipeError):
            pass  # Manager loss is handled separately by the worker lifecycle.
        return None

    _active_function_watchdog = _mark_function_call
    parent_pid = os.getppid()
    heartbeat_interval = 10.0
    last_heartbeat_check = time.time()
    last_activity_time = time.time()
    max_idle_time = None  # run forever unless set
    hb_miss_count = 0
    last_jobs_state = None
    cached_func = None
    cached_contract = None
    cached_brd_tests = None
    cached_n_items_to_pass = None
    cached_id_field = None
    cached_expected_field = None
    last_brd_hash = None
    last_reflection = None
    last_impl_meta = None

    # Blocked state: persisted in registry by BRD hash, remains until human changes BRD.
    blocked_hash: str | None = None
    blocked_reason: str | None = None
    next_block_warn_ts: float = 0.0  # throttle warnings to 20s
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
                    worker_logger.info("BRD content changed — invalidating cache for %s", brd_path.name)
                    # Only the current BRD's hash is relevant; old hash blocks stay archived
                    # until the manager's normal registry cleanup prunes them.
                    persisted_block = get_brd_runtime_block(registry_path, current_hash)
                    approved_recovery = get_approved_handcrafted_recovery(
                        registry_path, current_hash, saved_func_dir, brd_path
                    )
                    if persisted_block and not approved_recovery:
                        blocked_hash = current_hash
                        blocked_reason = persisted_block.get("reason", "human review required")
                    cached_func = None
                    cached_contract = None
                    cached_brd_tests = None
                    cached_n_items_to_pass = None
                    cached_id_field = None
                    cached_expected_field = None
                    next_deployment_preflight_warn_ts = 0.0
                    last_reflection = None
                    last_impl_meta = None
                    try:
                        shared_data["shared_dict"].pop(f"active_impl::{brd_path.name}", None)
                    except Exception:
                        pass
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
                    _yield_slot("blocked")
                    break

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
                        _yield_slot("wait_for_brd")
                        break
                else:
                    contract = cached_contract
                    brd_tests = cached_brd_tests
                    n_items_to_pass = cached_n_items_to_pass
                    id_field = cached_id_field
                    expected_field = cached_expected_field

                # ----------------- Python-owned function/job metadata -----------------
                schema = {
                    "function_name": brd_to_function_name(brd_path),
                    "inputs": [item["name"] for item in contract["input"]],
                    "incident_id": id_field,
                    "expected": expected_field,
                }
                if brd_changed:
                    module_logger.info(
                        "Python-derived metadata for %s: function=%s ID_FIELD=%s EXPECTED_FIELD=%s",
                        brd_path.name, schema["function_name"], id_field, expected_field,
                    )

                # ----------------- Ensure function -----------------
                # An unchanged registered implementation needs no labeled deployment jobs.
                # Only new/modified candidates require the N-job preflight and both gates.
                if cached_func is None:
                    jobs_path_for_deploy = brd_path.with_name(f"{brd_path.stem}_jobs.json")
                    try:
                        func, func_name, impl_meta = _get_or_create_function(
                            brd_path=brd_path,
                            brd_text=brd_text,
                            hash_signature=current_hash,
                            schema=schema,
                            contract=contract,
                            brd_tests=brd_tests,
                            n_items_to_pass=n_items_to_pass,
                            jobs_path=jobs_path_for_deploy,
                            saved_func_dir=saved_func_dir,
                            registry_path=registry_path,
                            gpt_semaphore=gpt_semaphore,
                            registry_lock=registry_lock,
                            logger=module_logger,
                            is_debug_mode=is_debug_mode,
                            reflection=last_reflection,
                            handcrafted_only=bool(approved_recovery),
                        )
                    except DeploymentValidationDataUnavailable as exc:
                        if now >= next_deployment_preflight_warn_ts:
                            module_logger.warning(
                                "Deployment preflight blocked for %s: %s. New/modified functions need "
                                "N_ITEMS_TO_PASS=%d labeled jobs; registered unchanged functions do not.",
                                brd_path.name, exc, n_items_to_pass,
                            )
                            next_deployment_preflight_warn_ts = now + warn_bad_BRD_every_x_seconds
                        _yield_slot("waiting_input")
                        break
                    except BlindDeploymentValidationFailure as exc:
                        # Do not retry, regenerate, or send any blind-test data to GPT.
                        blocked_hash = current_hash
                        blocked_reason = str(exc)
                        next_block_warn_ts = 0.0
                        last_reflection = None
                        block_brd_until_changed(
                            registry_path, current_hash, brd_path.name, blocked_reason,
                            registry_lock=registry_lock, origin="blind_deployment",
                        )
                        module_logger.error(
                            "Blind deployment validation stopped %s: %s "
                            "Waiting for human BRD review/change.",
                            brd_path.name, exc,
                        )
                        _yield_slot("blocked")
                        break
                    if func is None:
                        if approved_recovery:
                            finish_handcrafted_recovery(
                                registry_path, current_hash, saved_func_dir, brd_path,
                                successful=False, registry_lock=registry_lock,
                            )
                        # Persist this block: a restart must not silently grant
                        # another ten blind-validation/generation opportunities.
                        blocked_hash = current_hash
                        blocked_reason = f"no working function after up to {MAX_ATTEMPTS} generation attempts; human review required"
                        next_block_warn_ts = 0.0
                        block_brd_until_changed(
                            registry_path, current_hash, brd_path.name, blocked_reason,
                            registry_lock=registry_lock, origin="generation_exhausted",
                        )
                        module_logger.warning(
                            "Entering blocked mode for %s: %s. Will warn every %ss until BRD changes.",
                            brd_path.name, blocked_reason, warn_bad_BRD_every_x_seconds,
                        )
                        _yield_slot("blocked")
                        break

                    if approved_recovery:
                        # The manual candidate has passed both Python-owned gates.
                        # Activation requires exact byte match to the approved script.
                        finish_handcrafted_recovery(
                            registry_path, current_hash, saved_func_dir, brd_path,
                            successful=True, registry_lock=registry_lock,
                        )
                        module_logger.info(
                            "HUMAN RECOVERY APPROVED: %s passed BRD examples and %d N-job checks; persistent block lifted",
                            brd_path.name, n_items_to_pass,
                        )
                    cached_func = func
                    last_impl_meta = impl_meta or {"origin": "unknown", "script_name": "<unknown>"}
                    try:
                        shared_data["shared_dict"][f"active_impl::{brd_path.name}"] = {
                            "hash": current_hash,
                            "origin": last_impl_meta.get("origin", "unknown"),
                            "script_name": last_impl_meta.get("script_name", "<unknown>"),
                            "script_path": last_impl_meta.get("script_path"),
                        }
                    except Exception as exc:
                        module_logger.warning("Cannot publish active implementation metadata: %s", exc)
                    last_activity_time = now

                # ----------------- Jobs derivation -----------------
                jobs_schema = {
                    "inputs": [item["name"] for item in contract["input"]],
                    "expected": schema["expected"],
                    "incident_id": schema["incident_id"],
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
                            done_ids, rejected_hashes = _read_done_status(done_file)
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
                    except DoneStatePersistenceError as exc:
                        module_logger.error("Done-state unavailable for %s: %s; retrying without quarantining implementation", brd_path.name, exc)
                        _yield_slot("state_retry")
                        break
                    except Exception as exc:
                        module_logger.error("Error checking jobs for %s: %s", brd_path.name, exc, exc_info=is_debug_mode)
                        _yield_slot("waiting_input")
                        break
                else:
                    if last_jobs_state != "idle":
                        module_logger.info("No jobs file for %s yet (%s).", brd_path.name, jobs_path.name)
                        last_jobs_state = "idle"
                    has_new_jobs = False

                if not has_new_jobs:
                    _yield_slot("idle")
                    break

                # ----------------- Process jobs (if any) -----------------
                try:
                    ok_batch, reflection = process_jobs(
                        cached_func, jobs_schema, jobs_path, done_file,
                        output_contract=contract["output"],
                        input_specs=contract["input"],
                        compare_expected=True,
                        max_jobs=MAX_JOBS_PER_WORKER_SESSION,
                    )
                except DoneStatePersistenceError as exc:
                    module_logger.error("Done-state unavailable for %s: %s; retrying without quarantining implementation", brd_path.name, exc)
                    _yield_slot("state_retry")
                    break
                except JobDataTemporarilyUnavailable as exc:
                    module_logger.warning(
                        "Skipping this polling cycle for %s: %s; will retry. "
                        "Active function remains unchanged.", brd_path.name, exc,
                    )
                    _yield_slot("waiting_input")
                    break
                if ok_batch:
                    worker_logger.info("✅ Jobs processed for %s", brd_path.name)
                    last_reflection = None
                    last_activity_time = now
                    if len(new_jobs) > MAX_JOBS_PER_WORKER_SESSION:
                        _yield_slot("quantum")
                        break
                    time.sleep(check_interval)
                else:
                    # This is a failure of a valid live job, not malformed job data.
                    # Stop immediately: never auto-repair a deployed function via GPT.
                    worker_logger.error("❌ Live-job function failure for %s: %s", brd_path.name, reflection)
                    try:
                        if last_impl_meta and last_brd_hash == current_hash:
                            add_quarantine(
                                reg_path=registry_path,
                                hash_signature=current_hash,
                                script_name=last_impl_meta.get("script_name", "<unknown>"),
                                origin=last_impl_meta.get("origin", "unknown"),
                                reason=(reflection or "live-job function failure"),
                                registry_lock=registry_lock,
                                script_path=last_impl_meta.get("script_path"),
                            )
                    except Exception as q_exc:
                        module_logger.error("Could not quarantine implementation: %s", q_exc)

                    blocked_hash = current_hash
                    blocked_reason = "Deployed function failed on a valid live job; human review required"
                    next_block_warn_ts = 0.0
                    last_reflection = None
                    cached_func = None
                    last_impl_meta = None
                    try:
                        shared_data["shared_dict"].pop(f"active_impl::{brd_path.name}", None)
                    except Exception:
                        pass
                    try:
                        block_brd_until_changed(
                            registry_path, current_hash, brd_path.name, blocked_reason,
                            registry_lock=registry_lock, origin="live_job_failure",
                        )
                    except Exception as exc:
                        module_logger.error("Could not persist human-review block for %s: %s", brd_path.name, exc)
                    module_logger.error(
                        "ESCALATE TO HUMAN: BRD %s is blocked. "
                        "No automatic GPT repair or subsequent real-job processing.",
                        brd_path.name,
                    )
                    _yield_slot("blocked")
                    break


            except GPTServiceConfigurationError as exc:
                # Shared GPT configuration/policy failures are not BRD defects.
                # Keep the worker blocked in memory until restart (after an operator
                # repairs the environment or provider configuration). Never persist
                # a per-BRD hash block for a global GPT service misconfiguration.
                blocked_hash = current_hash
                blocked_reason = (
                    "GPT configuration/policy failure; fix the service settings "
                    f"and restart: {str(exc)[:250]}"
                )
                next_block_warn_ts = 0.0
                last_reflection = None
                module_logger.error("ESCALATE TO HUMAN: %s", blocked_reason)
                _yield_slot("blocked_configuration")
                break
            except ExternalGPTUnavailable as exc:
                module_logger.warning(
                    "External GPT service unavailable for %s (%s). "
                    "Will retry without quarantining or blocking the active function.",
                    brd_path.name, exc,
                )
                _yield_slot("service_retry")
                break
            except (BrokenPipeError, EOFError, OSError) as exc:
                try: worker_logger.info("IPC channel gone (%s) — exiting %s.", type(exc).__name__, brd_path.name)
                except Exception: pass
                break
            except KeyboardInterrupt:
                worker_logger.info("KeyboardInterrupt — exiting %s", brd_path.name)
                break
            except Exception as exc:
                if cached_func is not None and last_impl_meta and last_brd_hash:
                    # A valid active implementation was running when the
                    # unexpected loop exception occurred: fail closed for
                    # human investigation, not automatic LLM regeneration.
                    module_logger.exception(
                        "ESCALATE TO HUMAN: unexpected active-function loop failure for %s: %s",
                        brd_path.name, exc,
                    )
                    try:
                        add_quarantine(
                            reg_path=registry_path,
                            hash_signature=last_brd_hash,
                            script_name=last_impl_meta.get("script_name", "<unknown>"),
                            origin=last_impl_meta.get("origin", "unknown"),
                            reason=f"Unexpected active-function loop error: {type(exc).__name__}: {exc}",
                            registry_lock=registry_lock,
                            script_path=last_impl_meta.get("script_path"),
                        )
                    except Exception as quarantine_exc:
                        module_logger.error("Quarantine failed: %s", quarantine_exc)
                    blocked_hash = last_brd_hash
                    blocked_reason = "Unexpected exception while active function was running; human review required"
                    next_block_warn_ts = 0.0
                    last_reflection = None
                    cached_func = None
                    last_impl_meta = None
                    try:
                        shared_data["shared_dict"].pop(f"active_impl::{brd_path.name}", None)
                    except Exception:
                        pass
                    try:
                        block_brd_until_changed(
                            registry_path, blocked_hash, brd_path.name, blocked_reason,
                            registry_lock=registry_lock, origin="active_function_loop_error",
                        )
                    except Exception as persist_exc:
                        module_logger.error("Failed to persist active-function block: %s", persist_exc)
                    _yield_slot("blocked")
                    break

                module_logger.exception("Loop error for %s: %s", brd_path.name, exc)
                _yield_slot("wait_for_brd")
                break

    except Exception as exc:
        module_logger.exception("Fatal error in perpetual processing for %s: %s", brd_path.name, exc)
        raise
    finally:
        # No hanging/stale watchdog markers after a graceful shutdown.
        _active_function_watchdog = None
        _monitored_phase = None
        try:
            shared = shared_data["shared_dict"]
            key = f"function_call::{brd_path.name}"
            state = shared.get(key)
            if isinstance(state, dict) and state.get("pid") == os.getpid():
                shared.pop(key, None)
        except Exception:
            pass
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