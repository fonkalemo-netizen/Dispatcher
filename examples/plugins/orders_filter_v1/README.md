# orders_filter_v1 Plugin Template

This directory is a copyable example for authors who need to upload a Worker plugin.
Upload the `.py` file with `plugin_id=orders_filter_v1`. The file does not need
to be named `worker.py`; the platform stores the original safe filename in
plugin metadata.

## What To Change

- Pass your own `plugin_id` when uploading. It marks this Worker plugin.
- Replace `SOURCES` with the source names and configs your handler reads.
- Add `RESOURCES` only when your handler needs static config or a platform-managed snapshot.
- Keep `entrypoint` pointing to a function or actor class in this module.
- Return a small result dictionary from the handler.

## Required Shape

```python
SOURCES = {...}       # optional only if all sources are injected by the platform
RESOURCES = {...}     # optional
HANDLERS = [
    {
        "entrypoint": "run",
        "sources": ["source-name"],
        "resources": ["resource-name"],
        "batch_size": [1, 1000],
    }
]

def run(request, records, resources):
    ...
```

`HANDLERS` must be non-empty. For uploaded plugins, do not set `handler_id`.
The platform generates it as `{plugin_id}:{entrypoint}` after upload.

## Handler Signatures

```python
def run(request, records):
    ...

def run(request, records, resources):
    ...
```

For an uploaded plugin, Kafka records are decoded from JSON bytes/strings into
`dict` items before the handler is called. A single Kafka source receives
`list[dict]`; a multi-source Kafka handler receives a mapping like
`{"orders": list[dict], "payments": list[dict]}`. Postgres sources already
arrive as `list[dict]`.

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
