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
from openai import APIConnectionError, APIStatusError, RateLimitError
from dotenv import load_dotenv
from openpyxl import load_workbook
from typing import Callable, Any, List, Set, Tuple, Dict, Optional
import traceback
import tempfile
import ast
import logging, sys

_RETRY_STEPS = 6
MAX_ATTEMPTS = 10
_PERSIST_EVERY = 50
LOG = logging.getLogger("brd_processor")

# Load a developer-local .env file from the repository root if present.
# Existing shell/environment variables take precedence (override=False).
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
load_dotenv(_PROJECT_ROOT / ".env", override=False)

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
    """Return the persistent, BRD-hash-scoped human-review block, if any."""
    entry = load_registry(reg_path).get(hash_signature, {})
    block = entry.get("runtime_block") if isinstance(entry, dict) else None
    return block if isinstance(block, dict) else None


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
        if "runtime_block" not in entry:
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


_temperature_unsupported = False


def gpt_call_with_retry(semaphore, **kwargs):
    global _temperature_unsupported
    if gpt_client is None or not my_tools.gpt_model:
        raise RuntimeError(
            "Azure OpenAI is not configured. Set AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, "
            "and AZURE_OPENAI_DEPLOYMENT. AZURE_OPENAI_API_VERSION is optional."
        )

    call_kwargs = dict(kwargs)
    if _temperature_unsupported:
        call_kwargs.pop("temperature", None)

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
                _temperature_unsupported = True
                LOG.warning("Deployment rejected temperature; retrying without it (remembered for this worker).")
                continue
            if status != 429:
                LOG.exception("GPT hard error")
                raise
            LOG.warning("GPT 429 - backing off")
        except APIConnectionError as exc:
            LOG.warning("GPT connection error (%s) - backing off", type(exc).__name__)

        sleep = backoff + random.uniform(0, 0.3)
        LOG.warning("GPT retry in %.1fs", sleep)
        time.sleep(sleep)
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