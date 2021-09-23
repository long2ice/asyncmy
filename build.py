from distutils.command.build_ext import build_ext
from distutils.extension import Extension

from Cython.Build import cythonize


def build(setup_kwargs):
    ext = Extension(
        name="xstruct",
        sources=["asyncmy/struct.pyx"],
        library_dirs=["asyncmy/struct"],
        include_dirs=["asyncmy/struct"]
    )
    setup_kwargs.update(
        {
            "ext_modules": cythonize(
                [ext, "asyncmy/*.pyx"],
                compiler_directives={"language_level": 3},
            ),
            "cmdclass": {"build_ext": build_ext},
        }
    )
