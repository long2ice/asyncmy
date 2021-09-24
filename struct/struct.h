#ifndef STRUCT_H
#define STRUCT_H

#define STRUCT_ENDIAN_INCLUDED

#define STRUCT_ENDIAN_NOT_SET 0
#define STRUCT_ENDIAN_BIG 1
#define STRUCT_ENDIAN_LITTLE 2

int struct_get_endian(void);

extern int struct_pack(unsigned char *buf, const char *fmt, ...);
extern int struct_pack_into(int offset, unsigned char *buf, const char *fmt, ...);
extern int struct_unpack(unsigned char *buf, const char *fmt, ...);
extern int struct_unpack_from(int offset, unsigned char *buf, const char *fmt, ...);
extern int struct_calcsize(const char *fmt);

#endif
