cdef extern from "struct.h":
    int struct_pack(unsigned char *buf, const char *fmt, ...)
    int struct_unpack(unsigned char *buf, const char *fmt, ...)

cpdef pack(fmt: str, int[:] args):
    cdef unsigned char buf[100]
    struct_pack(buf, fmt.encode('utf-8'), args)
    return buf

cpdef unpack(fmt: str, char * arg):
    cdef unsigned char buf[100]
    struct_unpack(buf, fmt.encode('utf-8'), arg)
    return buf
