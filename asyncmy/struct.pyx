cdef extern from "struct.h":
    cpdef char * pack(char *fmt, ...)
    cpdef char * unpack(char *fmt, ...)

def pack(char * fmt, int[:] args):
    return pack(fmt, args)

def unpack(char * fmt, char * arg):
    return unpack(fmt, arg)
