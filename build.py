from Cython.Build import cythonize


def pdm_build_update_setup_kwargs(context, setup_kwargs) -> None:
    setup_kwargs["ext_modules"] = cythonize(
        ["asyncmy/*.pyx"],
        language_level="3",
    )
