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
    """Generate implementation code from the BRD's input/output contract."""
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

The FUNCTION INPUT/OUTPUT CONTRACT defines ordered inputs and the output schema.
Follow it exactly, including return type, nested structure, length constraints, and allowed values.
Do not change the contract or output structure. Your job is only to implement the function.

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


_ID_FIELD_RE = re.compile(r"^ID_FIELD=([A-Za-z_][A-Za-z0-9_]*)$")


def extract_job_directives(brd_text: str) -> dict[str, str]:
    """Read only the job ID field; legacy test-gate directives are ignored."""
    body = _extract_section_body(brd_text, "JOBS DATA STRUCTURE")
    lines = [line.strip() for line in body.splitlines() if line.strip().startswith("ID_FIELD")]
    if len(lines) != 1 or not _ID_FIELD_RE.fullmatch(lines[0]):
        raise ValueError("JOBS DATA STRUCTURE requires exactly one ID_FIELD=<identifier> line")
    return {"id_field": _ID_FIELD_RE.fullmatch(lines[0]).group(1)}


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

    An approved handwritten script can unblock its BRD after its exact bytes
    are verified and the callable loads. No example or real-job checks run.
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
        # silent source edit between approval and activation.
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
    if not id_field:
        raise ValueError("Runtime jobs require a Python-parsed ID_FIELD")

    if done_file is None:
        done_file = jobs_path.parent / f"done_{jobs_path.stem}.json"
    done_file.parent.mkdir(parents=True, exist_ok=True)

    try:
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
        # Do not report progress that was not durably recorded.
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

            done_ids.add(incident_id)
            # If a corrected record for this incident succeeds, clear older rejection entries for that ID.
            rejected = {
                key: value for key, value in rejected.items()
                if not isinstance(value, dict) or value.get("incident_id") != incident_id
            }
            _persist_done()  # Persist each successful job before reporting success.
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
        
def _get_or_create_function(
    brd_path: pathlib.Path,
    brd_text: str,
    hash_signature: str,
    schema: dict,
    contract: dict[str, Any],
    saved_func_dir: pathlib.Path,
    registry_path: pathlib.Path,
    gpt_semaphore: multiprocessing.Semaphore,
    registry_lock,
    logger,
    is_debug_mode: bool,
    handcrafted_only: bool = False,
):
    """Select handcrafted/cached/generated code without executing any approval tests."""
    expected_name = schema["function_name"]
    param_names = [spec["name"] for spec in contract["input"]]

    # [1] Handcrafted implementation (explicitly approved for blocked-BRD recovery).
    handcrafted = saved_func_dir / f"{brd_path.stem}_handcrafted"
    if handcrafted.exists():
        try:
            py_files = sorted(handcrafted.glob("*.py"))
            if len(py_files) != 1:
                raise RuntimeError(f"Expected exactly one .py in {handcrafted}; found {len(py_files)}")
            py_path = py_files[0]
            if not handcrafted_only and is_quarantined(registry_path, hash_signature, py_path.name, script_path=py_path):
                logger.info("Handcrafted implementation %s quarantined; skipping", py_path.name)
            else:
                mod = _invoke_monitored(load_script, namespace=f"hc_{hash_signature}", py_path=py_path, phase="handcrafted_code_load")
                if hasattr(mod, expected_name) and callable(getattr(mod, expected_name)):
                    func_name, func = expected_name, getattr(mod, expected_name)
                else:
                    public = [(n, f) for n, f in vars(mod).items()
                              if not n.startswith("_") and callable(f) and getattr(f, "__module__", None) == mod.__name__]
                    if len(public) != 1:
                        raise RuntimeError(f"Handcrafted module must define {expected_name}() or exactly one public function")
                    func_name, func = public[0]
                logger.info("Using handcrafted implementation %s", py_path.name)
                return func, func_name, {"origin": "handcrafted", "script_name": py_path.name, "script_path": str(py_path)}
        except (ExternalGPTUnavailable, GPTServiceConfigurationError):
            raise
        except Exception as exc:
            logger.warning("Handcrafted load failed: %s", exc)

    if handcrafted_only:
        logger.error("Approved handcrafted recovery cannot be loaded; no fallback allowed")
        return None, None, None

    # [2] Cached GPT implementation.
    meta = load_registry(registry_path).get(hash_signature, {})
    latest = meta.get("latest_script") if isinstance(meta, dict) else None
    if latest:
        cached_path = saved_func_dir / (meta.get("folder_name") or hash_signature) / latest
        func_name = meta.get("function_name") or expected_name
        try:
            if is_quarantined(registry_path, hash_signature, latest, script_path=cached_path):
                logger.info("Cached implementation %s quarantined; skipping", latest)
            else:
                _static_safety_check(cached_path.read_text(encoding="utf-8"))
                mod = _invoke_monitored(load_script, phase="cached_code_load", py_path=cached_path, namespace=f"saved_{hash_signature}")
                func = getattr(mod, func_name)
                if not callable(func):
                    raise RuntimeError(f"Cached {func_name} is not callable")
                logger.info("Reusing cached implementation %s", latest)
                return func, func_name, {"origin": "cached", "script_name": latest, "script_path": str(cached_path)}
        except (ExternalGPTUnavailable, GPTServiceConfigurationError):
            raise
        except Exception as exc:
            logger.warning("Cached implementation load failed: %s", exc)

    # [3] GPT implementation. Structural checks only; no examples or real-job gate.
    generation_contract = {"function_name": expected_name, "input": contract["input"], "output": contract["output"]}
    header = "Use this function contract exactly:\n" + json.dumps(generation_contract, ensure_ascii=False) + "\n\n"
    reflection = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info("GPT implementation attempt %d/%d for %s", attempt, MAX_ATTEMPTS, expected_name)
        try:
            resp = ask_gpt_with_naming_convention_to_make_func(
                gpt_semaphore, header + brd_text, expected_name, param_names, reflection,
            )
        except Exception as exc:
            kind = _classify_gpt_error(exc)
            if kind == "transient":
                raise ExternalGPTUnavailable(f"GPT code-generation service unavailable: {exc}") from exc
            if kind == "configuration":
                raise GPTServiceConfigurationError(f"GPT code-generation configuration error: {exc}") from exc
            reflection = f"GPT request rejected: {type(exc).__name__}: {exc}"
            continue
        if is_debug_mode:
            logger.info("GPT response:\n%s", resp)
        try:
            code = clean_generated_code(resp)
            _static_safety_check(code)
            namespace = {"my_tools": my_tools}
            _invoke_monitored(exec, code, namespace, phase="generated_code_load")
            func = namespace.get(expected_name)
            if not callable(func):
                raise ValueError(f"Generated {expected_name} is not callable")
        except (ExternalGPTUnavailable, GPTServiceConfigurationError):
            raise
        except Exception as exc:
            reflection = f"Generated code failed structural/load checks: {type(exc).__name__}: {exc}"
            logger.warning("GPT implementation attempt %d could not load: %s", attempt, exc)
            continue
        saved_name = save_source(hash_signature, code, expected_name, brd_path, saved_func_dir, registry_path, registry_lock)
        saved_path = saved_func_dir / brd_path.stem / saved_name
        logger.info("Generated implementation saved as %s", saved_name)
        return func, expected_name, {"origin": "gpt", "script_name": saved_name, "script_path": str(saved_path)}

    logger.error("Exhausted %d generation attempts for %s", MAX_ATTEMPTS, brd_path.name)
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
    Process one BRD until idle, blocked, or fair-use yield; manager relaunches on updates.

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
    cached_id_field = None
    last_brd_hash = None
    last_impl_meta = None

    # Blocked state: persisted in registry by BRD hash, remains until human changes BRD.
    blocked_hash: str | None = None
    blocked_reason: str | None = None
    next_block_warn_ts: float = 0.0  # throttle warnings to 20s

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
                    cached_id_field = None
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

                # ----------------- Python-owned BRD input/output contract -----------------
                if cached_contract is None or cached_id_field is None:
                    try:
                        contract = extract_brd_contract(brd_text)
                        id_field = extract_job_directives(brd_text)["id_field"]
                        cached_contract = contract
                        cached_id_field = id_field
                        module_logger.info("Parsed BRD %s inputs=%s ID_FIELD=%s", brd_path.name,
                                           [item["name"] for item in contract["input"]], id_field)
                    except Exception as exc:
                        blocked_hash = current_hash
                        blocked_reason = f"Invalid BRD input/output contract or job ID directive: {exc}"
                        module_logger.error("BRD metadata invalid for %s: %s", brd_path.name, exc)
                        _yield_slot("wait_for_brd")
                        break
                else:
                    contract, id_field = cached_contract, cached_id_field

                schema = {"function_name": brd_to_function_name(brd_path), "incident_id": id_field}
                if cached_func is None:
                    func, func_name, impl_meta = _get_or_create_function(
                        brd_path=brd_path,
                        brd_text=brd_text,
                        hash_signature=current_hash,
                        schema=schema,
                        contract=contract,
                        saved_func_dir=saved_func_dir,
                        registry_path=registry_path,
                        gpt_semaphore=gpt_semaphore,
                        registry_lock=registry_lock,
                        logger=module_logger,
                        is_debug_mode=is_debug_mode,
                        handcrafted_only=bool(approved_recovery),
                    )
                    if func is None:
                        if approved_recovery:
                            finish_handcrafted_recovery(
                                registry_path, current_hash, saved_func_dir, brd_path,
                                successful=False, registry_lock=registry_lock,
                            )
                        blocked_hash = current_hash
                        blocked_reason = "No loadable implementation; human review required"
                        block_brd_until_changed(
                            registry_path, current_hash, brd_path.name, blocked_reason,
                            registry_lock=registry_lock, origin="generation_exhausted",
                        )
                        _yield_slot("blocked")
                        break
                    if approved_recovery:
                        # Exact source bytes must match the human-approved version.
                        finish_handcrafted_recovery(
                            registry_path, current_hash, saved_func_dir, brd_path,
                            successful=True, registry_lock=registry_lock,
                        )
                        module_logger.info("Human-approved handcrafted recovery activated for %s", brd_path.name)
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