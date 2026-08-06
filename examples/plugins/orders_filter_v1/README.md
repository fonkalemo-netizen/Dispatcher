# orders_filter_v1 Plugin Template

This directory is a copyable example for authors who need to upload a Worker plugin.
Upload `worker.py` with `plugin_id=orders_filter_v1`.

## What To Change

- Replace every `orders_filter_v1` prefix with your own `plugin_id`.
- Keep `handler_id` explicit and prefixed: `{plugin_id}:{handler_name}`.
- Replace `SOURCES` with the source names and configs your handler reads.
- Add `RESOURCES` only when your handler needs static config or a platform-managed snapshot.
- Return a small result dictionary from the handler.

## Required Shape

```python
SOURCES = {...}       # optional only if all sources are injected by the platform
RESOURCES = {...}     # optional
HANDLERS = [
    {
        "handler_id": "your_plugin:run",
        "entrypoint": "run",
        "sources": ["source-name"],
        "resources": ["resource-name"],
        "batch_size": [1, 1000],
    }
]

def run(request, records, resources):
    ...
```

`HANDLERS` must be non-empty. For uploaded plugins, `handler_id` must be explicit and must start with the upload `plugin_id`.

## Handler Signatures

```python
def run(request, records):
    ...

def run(request, records, resources):
    ...
```

For a single source, `records` is a list. For a multi-source Kafka handler, `records` is a mapping like `{"orders": [...], "payments": [...]}`.

## Handler Result

The current plugin template does not assume a file sink or `request.output`.
Return a compact dictionary that is useful for observability:

```python
return {
    "handler": request.handler_id,
    "input_count": len(records),
    "output_count": len(filtered),
}
```

## Validation Rules

The plugin validator allows normal data-format imports such as `json`, `csv`, `io`, `pathlib`, `typing`, `datetime`, `decimal`, `math`, and `collections`.

It rejects dangerous APIs such as `eval`, `exec`, `compile`, `__import__`, `subprocess`, `socket`, `ctypes`, `importlib`, `multiprocessing`, `threading`, `shutil`, and system-level `os` calls.

Dangerous path literals such as `/etc`, `/root`, `~/.ssh`, or `..` are rejected. Other hardcoded absolute paths are warnings; runtime deployment should still restrict filesystem permissions.
