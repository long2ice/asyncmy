# Fast paths compare methods on exact native classes. Looking in their own
# class dictionary preserves monkeypatch detection without descriptor binding.
cdef extern from *:
    """
    static inline int asyncmy_class_method_is(
        PyObject *self, PyObject *name, PyObject *expected
    ) {
    #if defined(Py_GIL_DISABLED) || !CYTHON_COMPILING_IN_CPYTHON
        PyObject *actual = PyObject_GetAttr((PyObject *)Py_TYPE(self), name);
        if (actual == NULL) return -1;
        int matches = actual == expected;
        Py_DECREF(actual);
        return matches;
    #else
        PyObject *actual = PyDict_GetItemWithError(Py_TYPE(self)->tp_dict, name);
        if (actual == NULL && PyErr_Occurred()) return -1;
        return actual == expected;
    #endif
    }
    """
    bint _class_method_is "asyncmy_class_method_is"(object self, str name, object expected) except -1
