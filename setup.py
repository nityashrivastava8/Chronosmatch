"""Build the Cython matching engine:   python setup.py build_ext --inplace"""
import sys
from setuptools import Extension, setup
from Cython.Build import cythonize

flags = ["/O2"] if sys.platform == "win32" else ["-O3"]
ext = Extension("matching_engine", ["matching_engine.pyx"], extra_compile_args=flags)

setup(
    name="chronosmatch",
    ext_modules=cythonize(ext, annotate=True),   # annotate=True -> matching_engine.html
)
