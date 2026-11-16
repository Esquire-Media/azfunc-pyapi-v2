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
        generic_trigger = _identity

    class _Stub:
        def __init__(self, *args, **kwargs):
            pass

    class _OrchestrationRuntimeStatus:
        Completed = "Completed"
        Failed = "Failed"
        Canceled = "Canceled"
        Terminated = "Terminated"
        Running = "Running"
        Pending = "Pending"
        Suspended = "Suspended"
        Listening = "Listening"

    _stub.Blueprint = _Blueprint
    _stub.DurableOrchestrationClient = _Stub
    _stub.DurableOrchestrationContext = _Stub
    _stub.DurableClientContext = _Stub
    _stub.DurableOrchestrationStatus = _Stub
    _stub.EntityBackend = _Stub
    _stub.OrchestrationRuntimeStatus = _OrchestrationRuntimeStatus
    _stub.RetryOptions = _Stub
    _stub.TaskHubClient = _Stub
    _stub.WorkItem = _Stub
    sys.modules["azure.durable_functions"] = _stub
