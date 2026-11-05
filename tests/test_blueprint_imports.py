"""
Guard against a Function App failing to start because of a broken import.

``function_app.find_blueprints`` walks the blueprint paths for the deployed app
and executes every module it finds. There is no exception handling around
that, so a single module that cannot be imported aborts the whole app load and
the worker never indexes a single function - the app comes up with no
triggers at all, with nothing but the host log to explain it.

This test resolves each app's blueprint set from ``deployment.py`` and imports
every module, which is what the worker does at startup. A module that cannot
be imported because a repository module was renamed or deleted fails the test.
A module that cannot be imported because a third party package is not
installed is skipped, since that is a property of the environment rather than
of the repository.
"""

import importlib.util
import os
import sys

import pytest

from deployment import BLUEPRINTS

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve(paths):
    """Resolve a manifest entry the way function_app.find_blueprints does."""
    files = []
    for path in paths:
        if path.endswith("/*"):
            root, recursive = path[:-2], True
        elif path.endswith("/"):
            root, recursive = path[:-1], False
        else:
            files.append(path if path.endswith(".py") else path + ".py")
            continue
        for directory, _, names in os.walk(os.path.join(REPO_ROOT, root)):
            for name in names:
                if name.endswith(".py"):
                    files.append(
                        os.path.relpath(os.path.join(directory, name), REPO_ROOT)
                    )
            if not recursive:
                break
    return files


def _missing_repo_module(exc):
    """The repo-local module an ImportError refers to, if it is one."""
    name = getattr(exc, "name", None) or ""
    if name.startswith("libs"):
        return name
    return None


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    """Modules read these at import time."""
    monkeypatch.setenv("AzureWebJobsStorage", "UseDevelopmentStorage=true")
    monkeypatch.setenv(
        "DATABIND_SQL_KEYSTONE",
        "postgresql+psycopg2://user:pass@localhost:5432/legato",
    )


@pytest.mark.parametrize("app", sorted(BLUEPRINTS))
def test_app_blueprints_import(app, monkeypatch):
    """Every module an app loads must be importable."""
    failures = []
    skipped = []

    for index, relative in enumerate(sorted(_resolve(BLUEPRINTS[app]))):
        path = os.path.join(REPO_ROOT, relative)
        if not os.path.exists(path):
            # find_blueprints silently skips a manifest entry that does not
            # resolve, so this is worth reporting but is not a startup failure.
            skipped.append("{0}: listed in the manifest but missing".format(relative))
            continue

        # A unique module name, so that files such as datalake/activities/copy.py
        # cannot shadow a standard library module.
        name = "blueprint_check_{0}_{1}".format(app.replace("-", "_"), index)
        try:
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
        except ModuleNotFoundError as exc:
            missing = _missing_repo_module(exc)
            if missing:
                failures.append(
                    "{0}: imports {1}, which does not exist".format(relative, missing)
                )
            else:
                # A third party package that is not installed here. That is a
                # property of the environment rather than of the repository, so
                # it is reported but does not fail the test.
                skipped.append("{0}: {1}".format(relative, exc.name))
        except Exception as exc:  # noqa: BLE001 - report any import failure
            failures.append("{0}: {1}: {2}".format(relative, type(exc).__name__, exc))
        finally:
            sys.modules.pop(name, None)

    if skipped:
        print(
            "\n{0}: {1} module(s) skipped for missing optional packages".format(
                app, len(skipped)
            )
        )
        for entry in skipped:
            print("    {0}".format(entry))

    assert not failures, "broken imports in {0}:\n{1}".format(app, "\n".join(failures))


def test_no_app_loads_the_same_module_twice():
    """A module loaded twice in one app would register its functions twice.

    Two apps may share a module - they are deployed separately - but a single
    app must not resolve the same file through two of its manifest entries.
    """
    for app in sorted(BLUEPRINTS):
        resolved = _resolve(BLUEPRINTS[app])
        duplicates = {path for path in resolved if resolved.count(path) > 1}
        assert not duplicates, "{0} loads these modules twice: {1}".format(
            app, sorted(duplicates)
        )
