from distutils.command.build_ext import build_ext
from distutils.extension import Extension

from Cython.Build import cythonize


def build(setup_kwargs):
    setup_kwargs.update(
        {
            "ext_modules": cythonize(
                [
                    # Extension(
                    #     "xstruct",
                    #     sources=["asyncmy/struct.pyx"],
                    #     libraries=["struct"],
                    #     library_dirs=["struct"],
                    #     include_dirs=["struct"],
                    # ),
                    "asyncmy/*.pyx"
                ],
                compiler_directives={"language_level": 3},
            ),
            "cmdclass": {"build_ext": build_ext},
        }
    )
