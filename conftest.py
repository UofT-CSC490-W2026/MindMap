"""
Root conftest.py — runs before any test collection.

Fixes two problems:
1. 'from workers.xxx import' needs app/ on sys.path
2. 'from app.config import' inside workers gets confused by the namespace package
   conflict when both '.' and 'app/' are on sys.path simultaneously.

Solution: mock modal+snowflake first, import app.config cleanly, register it
under both 'app.config' and 'config', then add app/ to sys.path for bare imports.
"""
import sys
import os
from unittest.mock import MagicMock

# Step 1: Mock modal before anything else
if "modal" not in sys.modules:
    modal_mock = MagicMock()
    app_mock = MagicMock()
    modal_mock.App.return_value = app_mock

    def _passthrough(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator

    app_mock.function = _passthrough
    app_mock.cls = _passthrough
    app_mock.local_entrypoint = _passthrough
    modal_mock.Image.debian_slim = MagicMock(return_value=MagicMock(
        pip_install=MagicMock(return_value=MagicMock(
            add_local_file=MagicMock(return_value=MagicMock(
                add_local_dir=MagicMock(return_value=MagicMock(
                    env=MagicMock(return_value=MagicMock())
                ))
            ))
        ))
    ))
    modal_mock.Secret.from_name = MagicMock(return_value=MagicMock())
    modal_mock.method = _passthrough
    modal_mock.enter = _passthrough
    modal_mock.asgi_app = _passthrough
    sys.modules["modal"] = modal_mock

# Step 2: Protect numpy from being mocked (hypothesis needs the real numpy.random)
try:
    import numpy as _numpy
    import numpy.random as _numpy_random
    sys.modules["numpy"] = _numpy
    sys.modules["numpy.random"] = _numpy_random
except ImportError:
    pass

# Mock snowflake
if "snowflake" not in sys.modules:
    sf_mock = MagicMock()
    sys.modules["snowflake"] = sf_mock
    sys.modules["snowflake.connector"] = sf_mock.connector

# Clean up any numpy mocks that may have been pulled in transitively
for _key in list(sys.modules):
    if (_key == "numpy" or _key.startswith("numpy.")) and isinstance(sys.modules[_key], MagicMock):
        del sys.modules[_key]

# Step 3: Import app.config cleanly while only '.' is on sys.path,
# then register under both names before adding 'app/' to sys.path
if "app.config" not in sys.modules:
    import app.config as _real_config
    sys.modules["config"] = _real_config

# Also pre-register app.workers modules that use 'from app.workers.xxx import'
# to avoid namespace conflicts after app/ is added to sys.path
if "app.workers.semantic_search_worker" not in sys.modules:
    import app.workers.semantic_search_worker as _ssw
    sys.modules["workers.semantic_search_worker"] = _ssw

# Step 4: Now add app/ so 'from workers.xxx import' works
app_dir = os.path.join(os.path.dirname(__file__), "app")
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)
