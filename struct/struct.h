#ifndef STRUCT_H
#define STRUCT_H

#define STRUCT_ENDIAN_INCLUDED

#define STRUCT_ENDIAN_NOT_SET 0
#define STRUCT_ENDIAN_BIG 1
#define STRUCT_ENDIAN_LITTLE 2

int struct_get_endian(void);

char *struct_pack(char *fmt, ...);

int struct_pack_into(int offset, void *buf, const char *fmt, ...);

char *struct_unpack(char *fmt, ...);

int struct_unpack_from(
    int offset,
    const void *buf,
    const char *fmt,
    ...);

int struct_calcsize(const char *fmt);

#endif
