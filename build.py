from distutils.command.build_ext import build_ext  # type:ignore

from Cython.Build import cythonize


def build(setup_kwargs):
    setup_kwargs.update(
        {
            "ext_modules": cythonize(
                [
                    "asyncmy/*.pyx",
                ],
                compiler_directives={"language_level": 3},
            ),
            "cmdclass": {"build_ext": build_ext},
        }
    )
