"""Validate uploaded worker plugins before they enter the dispatcher.

AST layer blocks dangerous APIs and flags risky paths. Path isolation under
``request.output["path"]`` is enforced at deploy/runtime, not proven by AST.
"""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

# Sequence kept for type hints in public helpers if needed.

PLUGIN_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
DEFAULT_MAX_BYTES = 1_048_576  # 1 MiB
DEFAULT_DISCOVER_TIMEOUT = 3.0

ALLOWED_IMPORT_ROOTS = frozenset(
    {
        "__future__",
        "json",
        "csv",
        "io",
        "math",
        "datetime",
        "decimal",
        "typing",
        "pathlib",
        "collections",
        "dataclasses",
        "enum",
        "functools",
        "itertools",
        "re",
        "copy",
        "hashlib",
        "base64",
        "uuid",
        "time",
        "string",
        "textwrap",
        "contextlib",
        "abc",
        "numbers",
        "operator",
        "statistics",
    }
)

FORBIDDEN_IMPORT_ROOTS = frozenset(
    {
        "os",
        "sys",
        "subprocess",
        "socket",
        "ctypes",
        "importlib",
        "multiprocessing",
        "threading",
        "requests",
        "urllib",
        "http",
        "asyncio",
        "shutil",
        "pickle",
        "marshal",
        "builtins",
        "code",
        "codeop",
        "pty",
        "fcntl",
        "signal",
        "resource",
        "tempfile",
        "glob",
    }
)

FORBIDDEN_NAMES = frozenset(
    {
        "eval",
        "exec",
        "compile",
        "__import__",
        "breakpoint",
        "input",
        "exit",
        "quit",
    }
)

FORBIDDEN_ATTR_CHAINS = frozenset(
    {
        ("os", "system"),
        ("os", "popen"),
        ("os", "remove"),
        ("os", "unlink"),
        ("os", "rename"),
        ("os", "replace"),
        ("os", "execl"),
        ("os", "execv"),
        ("os", "spawn"),
        ("os", "spawnl"),
        ("os", "spawnv"),
        ("subprocess", "run"),
        ("subprocess", "Popen"),
        ("subprocess", "call"),
        ("subprocess", "check_call"),
        ("subprocess", "check_output"),
        ("shutil", "rmtree"),
        ("shutil", "move"),
        ("shutil", "copy"),
        ("shutil", "copy2"),
        ("shutil", "copytree"),
        ("pathlib", "Path", "unlink"),
        ("pathlib", "Path", "rmdir"),
        ("Path", "unlink"),
        ("Path", "rmdir"),
    }
)

DANGEROUS_PATH_PREFIXES = (
    "/etc",
    "/root",
    "/proc",
    "/sys",
    "/dev",
    "/var/run/secrets",
)
DANGEROUS_PATH_PARTS = (".ssh", "..")


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    handler_ids: list[str] = field(default_factory=list)
    source_ids: list[str] = field(default_factory=list)
    resource_ids: list[str] = field(default_factory=list)
    sha256: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "handler_ids": list(self.handler_ids),
            "source_ids": list(self.source_ids),
            "resource_ids": list(self.resource_ids),
            "sha256": self.sha256,
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_plugin_id(plugin_id: str) -> None:
    if not plugin_id or not PLUGIN_ID_RE.fullmatch(plugin_id):
        raise ValueError(
            "plugin_id must match [a-zA-Z0-9_-]+ and must not be empty"
        )


def validate_worker_py(
    path: Path | str,
    *,
    plugin_id: str,
    max_bytes: int = DEFAULT_MAX_BYTES,
    discover_timeout: float = DEFAULT_DISCOVER_TIMEOUT,
    run_discover: bool = True,
) -> ValidationResult:
    """Validate a staging plugin Python file for ``plugin_id``."""

    validate_plugin_id(plugin_id)
    target = Path(path)
    errors: list[str] = []
    warnings: list[str] = []
    handler_ids: list[str] = []
    source_ids: list[str] = []
    resource_ids: list[str] = []
    digest = ""

    if target.suffix != ".py":
        errors.append(f"plugin file must be a .py file, got {target.name!r}")
    if not target.is_file():
        errors.append(f"plugin file does not exist: {target}")
        return ValidationResult(False, errors, warnings)

    size = target.stat().st_size
    if size > max_bytes:
        errors.append(f"plugin file exceeds size limit ({size} > {max_bytes})")
    if size == 0:
        errors.append("plugin file is empty")

    try:
        digest = sha256_file(target)
        source = target.read_text(encoding="utf-8")
    except OSError as exc:
        errors.append(f"cannot read plugin file: {exc}")
        return ValidationResult(False, errors, warnings, sha256=digest)

    try:
        tree = ast.parse(source, filename=str(target))
    except SyntaxError as exc:
        errors.append(f"syntax error: {exc}")
        return ValidationResult(False, errors, warnings, sha256=digest)

    ast_errors, ast_warnings = _ast_scan(tree)
    errors.extend(ast_errors)
    warnings.extend(ast_warnings)

    structure = _structure_from_ast(tree, plugin_id)
    errors.extend(structure["errors"])
    warnings.extend(structure["warnings"])
    handler_ids = list(structure["handler_ids"])
    source_ids = list(structure["source_ids"])
    resource_ids = list(structure["resource_ids"])

    if run_discover and not errors:
        discovered = _subprocess_discover(target.parent, timeout=discover_timeout)
        errors.extend(discovered["errors"])
        if discovered["handler_ids"] and not handler_ids:
            handler_ids = [
                f"{plugin_id}:{str(handler_id).rsplit(':', 1)[-1]}"
                for handler_id in discovered["handler_ids"]
            ]
        if discovered["source_ids"]:
            source_ids = discovered["source_ids"]
        if discovered["resource_ids"]:
            resource_ids = discovered["resource_ids"]
        for handler_id in handler_ids:
            prefix = f"{plugin_id}:"
            if not handler_id.startswith(prefix):
                errors.append(
                    f"generated handler_id {handler_id!r} must start with {prefix!r}"
                )

    return ValidationResult(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        handler_ids=handler_ids,
        source_ids=source_ids,
        resource_ids=resource_ids,
        sha256=digest,
    )


def _ast_scan(tree: ast.AST) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    class Visitor(ast.NodeVisitor):
        def visit_Import(self, node: ast.Import) -> None:
            for alias in node.names:
                root = alias.name.split(".", 1)[0]
                self._check_import(root, node.lineno)
            self.generic_visit(node)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            if node.level and node.level > 0:
                errors.append(
                    f"line {node.lineno}: relative imports are not allowed"
                )
            module = node.module or ""
            root = module.split(".", 1)[0] if module else ""
            if root:
                self._check_import(root, node.lineno)
            self.generic_visit(node)

        def visit_Call(self, node: ast.Call) -> None:
            name = _call_name(node.func)
            if name in FORBIDDEN_NAMES:
                errors.append(f"line {node.lineno}: forbidden call {name}()")
            chain = tuple(_attr_chain(node.func))
            for forbidden in FORBIDDEN_ATTR_CHAINS:
                if chain[: len(forbidden)] == forbidden:
                    errors.append(
                        f"line {node.lineno}: forbidden call {'.'.join(chain)}"
                    )
                    break
            if chain and chain[-1] in {"unlink", "rmdir"} and "Path" in chain:
                errors.append(
                    f"line {node.lineno}: pathlib delete APIs are not allowed"
                )
            if _is_open_call(node):
                self._check_open(node)
            self.generic_visit(node)

        def visit_Name(self, node: ast.Name) -> None:
            if isinstance(node.ctx, ast.Load) and node.id in FORBIDDEN_NAMES:
                # bare name reference; calls handled above
                pass
            self.generic_visit(node)

        def visit_Attribute(self, node: ast.Attribute) -> None:
            chain = _attr_chain(node)
            if len(chain) >= 2 and (chain[0], chain[1]) in {
                ("os", "system"),
                ("os", "popen"),
                ("os", "remove"),
                ("os", "unlink"),
                ("os", "rename"),
                ("os", "replace"),
            }:
                errors.append(
                    f"line {node.lineno}: forbidden attribute {'.'.join(chain[:2])}"
                )
            self.generic_visit(node)

        def visit_Constant(self, node: ast.Constant) -> None:
            if isinstance(node.value, str):
                kind = _path_literal_kind(node.value)
                if kind == "danger":
                    errors.append(
                        f"line {node.lineno}: dangerous path literal {node.value!r}"
                    )
                elif kind == "absolute":
                    warnings.append(
                        f"line {node.lineno}: hardcoded absolute path "
                        f"{node.value!r}; prefer request.output['path']"
                    )
            self.generic_visit(node)

        def _check_import(self, root: str, lineno: int) -> None:
            if root == "__future__":
                return
            if root in FORBIDDEN_IMPORT_ROOTS:
                errors.append(f"line {lineno}: forbidden import {root!r}")
                return
            if root.startswith("_"):
                errors.append(f"line {lineno}: forbidden import {root!r}")
                return
            if root not in ALLOWED_IMPORT_ROOTS:
                errors.append(
                    f"line {lineno}: import {root!r} is not on the allow-list"
                )

        def _check_open(self, node: ast.Call) -> None:
            mode = _open_mode(node)
            if mode is None:
                return
            if any(flag in mode for flag in ("w", "a", "x", "+")):
                # Controlled write is allowed; path safety is runtime-enforced.
                return

    Visitor().visit(tree)
    return errors, warnings


def _is_open_call(node: ast.Call) -> bool:
    if isinstance(node.func, ast.Name) and node.func.id == "open":
        return True
    chain = _attr_chain(node.func)
    return chain[-2:] == ["Path", "open"] or chain[-1:] == ["open"]


def _open_mode(node: ast.Call) -> str | None:
    if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
        if isinstance(node.args[1].value, str):
            return node.args[1].value
    for keyword in node.keywords:
        if keyword.arg == "mode" and isinstance(keyword.value, ast.Constant):
            if isinstance(keyword.value.value, str):
                return keyword.value.value
    # default read mode
    return "r"


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return None


def _attr_chain(node: ast.AST) -> list[str]:
    parts: list[str] = []
    current: ast.AST | None = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    parts.reverse()
    return parts


def _path_literal_kind(value: str) -> str | None:
    if ".." in Path(value).parts or ".." in value:
        return "danger"
    lowered = value.replace("\\", "/").lower()
    if any(part == ".ssh" for part in Path(value).parts):
        return "danger"
    for prefix in DANGEROUS_PATH_PREFIXES:
        if lowered == prefix or lowered.startswith(prefix + "/"):
            return "danger"
    if value.startswith("~"):
        return "danger"
    if value.startswith("/") or (len(value) >= 3 and value[1:3] == ":\\"):
        return "absolute"
    return None


def _structure_from_ast(tree: ast.Module, plugin_id: str) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    handler_ids: list[str] = []
    source_ids: list[str] = []
    resource_ids: list[str] = []
    has_handlers = False
    top_level_symbols: set[str] = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            if target.id == "HANDLERS":
                has_handlers = True
                parsed = _parse_handlers_assign(
                    node.value,
                    plugin_id,
                    top_level_symbols=top_level_symbols,
                )
                errors.extend(parsed["errors"])
                warnings.extend(parsed["warnings"])
                handler_ids.extend(parsed["handler_ids"])
            elif target.id == "SOURCES" and isinstance(node.value, ast.Dict):
                source_ids.extend(_dict_string_keys(node.value))
            elif target.id == "RESOURCES" and isinstance(node.value, ast.Dict):
                resource_ids.extend(_dict_string_keys(node.value))
                parsed = _parse_resources_assign(
                    node.value,
                    top_level_symbols=top_level_symbols,
                )
                errors.extend(parsed["errors"])

    if not has_handlers:
        errors.append("module must assign HANDLERS")
    elif not handler_ids and not errors:
        # HANDLERS present but empty or non-literal — discover will decide.
        warnings.append("HANDLERS could not be fully inspected via AST literals")

    return {
        "errors": errors,
        "warnings": warnings,
        "handler_ids": handler_ids,
        "source_ids": source_ids,
        "resource_ids": resource_ids,
    }


def _dict_string_keys(node: ast.Dict) -> list[str]:
    keys: list[str] = []
    for key in node.keys:
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            keys.append(key.value)
    return keys


def _parse_resources_assign(
    node: ast.Dict,
    *,
    top_level_symbols: set[str],
) -> dict[str, Any]:
    errors: list[str] = []
    for index, value in enumerate(node.values):
        if not isinstance(value, ast.Dict):
            continue
        mapping = _dict_literal_strings(value)
        formatter = mapping.get("formatter")
        if formatter and formatter not in top_level_symbols:
            errors.append(
                f"RESOURCES[{index}] formatter {formatter!r} is not defined "
                "in this module"
            )
    return {"errors": errors}


def _parse_handlers_assign(
    node: ast.AST,
    plugin_id: str,
    *,
    top_level_symbols: set[str],
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    handler_ids: list[str] = []
    if not isinstance(node, (ast.List, ast.Tuple)):
        warnings.append("HANDLERS is not a list/tuple literal; deferring to discover")
        return {
            "errors": errors,
            "warnings": warnings,
            "handler_ids": handler_ids,
        }
    if not node.elts:
        errors.append("HANDLERS must not be empty")
        return {
            "errors": errors,
            "warnings": warnings,
            "handler_ids": handler_ids,
        }
    for index, elt in enumerate(node.elts):
        if not isinstance(elt, ast.Dict):
            warnings.append(f"HANDLERS[{index}] is not a dict literal")
            continue
        mapping = _dict_literal_strings(elt)
        if "handler_id" in mapping:
            errors.append(
                f"HANDLERS[{index}] must not set handler_id; "
                "uploaded plugin handler IDs are generated by the platform"
            )
        entrypoint = mapping.get("entrypoint")
        if entrypoint is None:
            # may still be present as non-constant; discover enforces
            warnings.append(f"HANDLERS[{index}] entrypoint not visible as literal")
            continue
        if entrypoint not in top_level_symbols:
            errors.append(
                f"HANDLERS[{index}] entrypoint {entrypoint!r} is not defined "
                "in this module"
            )
        handler_ids.append(f"{plugin_id}:{entrypoint}")
    return {
        "errors": errors,
        "warnings": warnings,
        "handler_ids": handler_ids,
    }


def _dict_literal_strings(node: ast.Dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in zip(node.keys, node.values):
        if (
            isinstance(key, ast.Constant)
            and isinstance(key.value, str)
            and isinstance(value, ast.Constant)
            and isinstance(value.value, str)
        ):
            out[key.value] = value.value
    return out


def _subprocess_discover(
    staging_dir: Path, *, timeout: float
) -> dict[str, Any]:
    import os

    errors: list[str] = []
    script = r"""
import json
import sys
from pathlib import Path
from ray_dispatcher.discovery import WorkerDiscoveryError, discover_workers

root = Path(sys.argv[1])
try:
    workers, sources, resources = discover_workers(root, ray_module=None)
except WorkerDiscoveryError as exc:
    print(json.dumps({"ok": False, "error": str(exc)}))
    raise SystemExit(2)
except Exception as exc:
    print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
    raise SystemExit(2)
print(
    json.dumps(
        {
            "ok": True,
            "handler_ids": [worker.name for worker in workers],
            "source_ids": sorted(sources),
            "resource_ids": sorted(resources),
        }
    )
)
"""
    package_root = Path(__file__).resolve().parents[2]
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().endswith(
            ("_TOKEN", "_SECRET", "_PASSWORD", "_KEY", "AWS_SECRET_ACCESS_KEY")
        )
    }
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(package_root)
        if not existing
        else str(package_root) + os.pathsep + existing
    )
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    try:
        completed = subprocess.run(
            [sys.executable, "-c", script, str(staging_dir.resolve())],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(staging_dir.resolve()),
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "errors": [f"discover timed out after {timeout}s"],
            "handler_ids": [],
            "source_ids": [],
            "resource_ids": [],
        }

    stdout = (completed.stdout or "")[-4000:]
    stderr = (completed.stderr or "")[-4000:]
    if completed.returncode != 0:
        message = stdout.strip() or stderr.strip() or f"exit {completed.returncode}"
        try:
            import json

            payload = json.loads(stdout.strip().splitlines()[-1])
            message = str(payload.get("error") or message)
        except Exception:
            pass
        errors.append(f"subprocess discover failed: {message}")
        return {
            "errors": errors,
            "handler_ids": [],
            "source_ids": [],
            "resource_ids": [],
        }

    try:
        import json

        payload = json.loads(stdout.strip().splitlines()[-1])
    except Exception as exc:
        errors.append(f"invalid discover output: {exc}; stderr={stderr!r}")
        return {
            "errors": errors,
            "handler_ids": [],
            "source_ids": [],
            "resource_ids": [],
        }
    return {
        "errors": [],
        "handler_ids": list(payload.get("handler_ids") or []),
        "source_ids": list(payload.get("source_ids") or []),
        "resource_ids": list(payload.get("resource_ids") or []),
    }


__all__ = [
    "ALLOWED_IMPORT_ROOTS",
    "DEFAULT_DISCOVER_TIMEOUT",
    "DEFAULT_MAX_BYTES",
    "ValidationResult",
    "sha256_file",
    "validate_plugin_id",
    "validate_worker_py",
]
