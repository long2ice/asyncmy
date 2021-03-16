from Cython.Build import cythonize
from distutils.command.build_ext import build_ext


def build(setup_kwargs):
    setup_kwargs.update({
        "ext_modules": cythonize(["asyncmy/*.pyx"]),
        "cmdclass": {"build_ext": build_ext},
        "language_level": 3
    })
