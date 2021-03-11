cimport cstruct


cdef bytes pack(str fmt, int data):
    cdef unsigned char *buf
    cstruct.struct_pack(buf, fmt, data)
    return buf

cdef bytes unpack(str fmt, int data):
    cdef unsigned char *buf
    cstruct.struct_unpack(buf, fmt, data)
    return buf
