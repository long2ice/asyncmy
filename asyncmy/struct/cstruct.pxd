cdef extern from "struct.h":
    int struct_pack(unsigned char *buf, const char *fmt, ...)
    int struct_pack_into(int offset, unsigned char *buf, const char *fmt, ...)
    int struct_unpack(unsigned char *buf, const char *fmt, ...)
    int struct_unpack_from(int offset, unsigned char *buf, const char *fmt, ...)
    int struct_calcsize(const char *fmt, ...)
