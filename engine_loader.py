"""Import the compiled Cython engine if it has been built, else fall back to
the pure-Python one so every script still runs."""
try:
    from matching_engine import MatchingEngine, clock_ns   # compiled C extension
    BACKEND = "cython"
except ImportError:
    from engine_py import MatchingEngine, clock_ns
    BACKEND = "python"
