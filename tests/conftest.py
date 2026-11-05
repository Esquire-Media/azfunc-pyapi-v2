"""
Shared pytest configuration.

The azure-durable-functions package is not always installable in a
development environment, but the blueprint modules import it at module scope.
This installs a minimal stand-in when the real package is missing, so the
pure logic of those modules can still be tested.
"""

import sys
import types

try:  # pragma: no cover - only runs when the SDK is absent
    import azure.durable_functions  # noqa: F401
except ImportError:  # pragma: no cover
    _stub = types.ModuleType("azure.durable_functions")

    class _Blueprint:
        def __init__(self, *args, **kwargs):
            pass

        def _identity(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        activity_trigger = _identity
        orchestration_trigger = _identity
        route = _identity
        durable_client_input = _identity
        timer_trigger = _identity

    _stub.Blueprint = _Blueprint
    sys.modules["azure.durable_functions"] = _stub
