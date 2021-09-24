cdef extern from "struct.h":
    char * struct_pack(char *fmt, ...)
    char * struct_unpack(char *fmt, ...)

def pack(char * fmt, int[:] args):
    return struct_pack(fmt, args)

def unpack(char * fmt, char * arg):
    return struct_unpack(fmt, arg)
