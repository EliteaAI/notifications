"""Shared fixtures: load plugin modules without the Pylon runtime.

Run with:
    pytest tests -q
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _install_pylon_stubs():
    class _Log:
        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    pylon = types.ModuleType('pylon')
    core = types.ModuleType('pylon.core')
    core_tools = types.ModuleType('pylon.core.tools')
    core_tools.log = _Log()
    core.tools = core_tools
    pylon.core = core

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace()
    tools.config = types.SimpleNamespace(POSTGRES_SCHEMA='centry')

    sys.modules.setdefault('pylon', pylon)
    sys.modules.setdefault('pylon.core', core)
    sys.modules.setdefault('pylon.core.tools', core_tools)
    sys.modules.setdefault('tools', tools)


def _load_module(module_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='session')
def db_tasks():
    _install_pylon_stubs()
    return _load_module(PLUGIN_ROOT / 'tasks' / 'db_tasks.py', 'notifications_db_tasks')
