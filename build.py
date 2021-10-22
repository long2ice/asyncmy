from distutils.command.build_ext import build_ext  # type:ignore
from distutils.extension import Extension

from Cython.Build import cythonize

extensions = [
    Extension(
        name='asyncmy',
        sources=[
            "asyncmy/*.pyx",
            "asyncmy/sa/*.pyx",
        ],
    )
]


def build(setup_kwargs):
    setup_kwargs.update(
        {
            "ext_modules": cythonize(
                [
                    "asyncmy/*.pyx",
                    "asyncmy/sa/*.pyx",
                ],
                compiler_directives={
                    "language_level": 3
                },
            ),
            "cmdclass": {
                "build_ext": build_ext
            },
        }
    )
