from contextlib import contextmanager
from typing import Any, Iterator

from ..logger import log_debug


class AdapterUnavailable(RuntimeError):
    """No usable dbt adapter is registered in this process."""


def acquire_in_process_adapter(adapter_type: str | None = None) -> tuple[Any, Any]:
    """Return the (adapter, manifest) dbt already set up in this process.

    `dbt source freshness` runs in-process via `dbtRunner` before we get here, and its
    `@requires.manifest` decorator registers the adapter and attaches the parsed manifest as
    the adapter's macro resolver *before* the task body runs -- so this works even for a
    project with no sources. We need neither a fresh parse nor our own profile/config
    bootstrap.

    The "exactly one adapter" assumption below holds by construction, not because dbt
    preserves the registry between invocations: `adapter_management()` calls
    `reset_adapters()` on *entry* to every `dbtRunner.invoke()`, clearing whatever the
    previous in-process invocation (`dbt ls`) registered, then `register_adapter()` fills it
    back in for that invocation's own `--target`. Since `dbt source freshness` is the last
    in-process call before we get here, its adapter -- registered against the same target we
    resolved for it -- is what's left in `FACTORY.adapters`. This is fragile to call-order
    changes: if another in-process dbtRunner invocation ran after freshness but before this
    function, it would silently replace what we read here.

    Raises AdapterUnavailable when dbt never got that far.
    """
    try:
        from dbt.adapters.factory import FACTORY, get_adapter_by_type
    except ImportError as missing_dbt_core_error:
        raise AdapterUnavailable(
            f"dbt Core is not importable: {missing_dbt_core_error}"
        ) from missing_dbt_core_error

    if adapter_type is None:
        # dbt registers exactly one adapter per invocation.
        registered = list(FACTORY.adapters)
        if len(registered) != 1:
            raise AdapterUnavailable(
                f"Expected exactly one registered dbt adapter, found {registered or 'none'}."
            )
        adapter_type = registered[0]

    try:
        adapter = get_adapter_by_type(adapter_type)
    except Exception as e:
        raise AdapterUnavailable(
            f"No dbt adapter registered for '{adapter_type}': {e}"
        ) from e

    manifest = adapter.get_macro_resolver()
    if manifest is None or not getattr(manifest, "nodes", None):
        raise AdapterUnavailable(
            "The registered dbt adapter has no manifest attached, so warehouse macros "
            "cannot be resolved."
        )

    return adapter, manifest


@contextmanager
def adapter_connection(
    adapter: Any, name: str = "orchestra_relation_existence"
) -> Iterator[None]:
    """Borrow a connection, and always hand it back.

    The real dbt run is a subprocess started straight after this, so we must not leave a
    connection open behind us.
    """
    try:
        with adapter.connection_named(name):
            adapter.clear_transaction()
            yield
    finally:
        try:
            adapter.cleanup_connections()
        except Exception as e:
            log_debug(f"Could not clean up warehouse connections: {e}")
