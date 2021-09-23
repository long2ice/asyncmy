#ifndef STRUCT_INCLUDED
#define STRUCT_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#define STRUCT_ENDIAN_INCLUDED

#define STRUCT_ENDIAN_NOT_SET   0
#define STRUCT_ENDIAN_BIG       1
#define STRUCT_ENDIAN_LITTLE    2

extern int struct_get_endian(void);

/**
 * @brief pack data
 * @return the number of bytes encoded on success, -1 on failure.
 */
extern char* pack(const char *fmt, ...);

/**
 * @brief pack data with offset
 * @return the number of bytes encoded on success, -1 on failure.
 */
extern int struct_pack_into(int offset, void *buf, const char *fmt, ...);

/**
 * @brief unpack data
 * @return the number of bytes decoded on success, -1 on failure.
 */
extern char* unpack(char *fmt, ...);

/**
 * @brief unpack data with offset
 * @return the number of bytes decoded on success, -1 on failure.
 */
extern int struct_unpack_from(
    int offset,
    const void *buf,
    const char *fmt,
    ...);

/**
 * @brief calculate the size of a format string
 * @return the number of bytes needed by the format string on success,
 * -1 on failure.
 *
 * make sure that the return value is > 0, before using it.
 */
extern int struct_calcsize(const char *fmt);

#ifdef __cplusplus
}
#endif

#endif /* !STRUCT_INCLUDED */
