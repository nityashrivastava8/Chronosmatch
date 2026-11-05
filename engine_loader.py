"""Pick the matching engine: the compiled Cython one if it has been built,
otherwise the pure-Python one (so every script still runs).

importlib is used on purpose: it keeps editors (Pylance) from flagging an
'unresolved import' while the Cython module has not been compiled yet."""
import importlib

try:
    _engine = importlib.import_module("matching_engine")    # compiled C extension
    BACKEND = "cython"
except ImportError:
    _engine = importlib.import_module("engine_py")          # pure-Python fallback
    BACKEND = "python"

MatchingEngine = _engine.MatchingEngine
clock_ns = _engine.clock_ns
