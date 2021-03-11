/*
 * struct.h
 *
 *  Created on: 2011. 5. 2.
 *      Author: Wonseok
 *
 * stolen from https://github.com/svperbeast/struct
 * 
 * modify:
 *    - pack/unpack Null-terminated string
 *    - pack/unpack blob data
 *    - pack/unpack *struct timeb*
 *
 * Interpret strings as packed binary data
 *
 *
 * Table 1. Byte order
 *  ----------------------------------
 *  Character | Byte order
 *  ----------+-----------------------
 *   =        | native
 *  ----------+-----------------------
 *   <        | little-endian
 *  ----------+-----------------------
 *   >        | big-endian
 *  ----------+-----------------------
 *   !        | network (= big-endian)
 *  ----------------------------------
 *
 * Table 2. Format characters
 *  -------------------------------------------
 *  Format | C/C++ Type         | Standard size
 *  -------+--------------------+--------------
 *   b     | char               | 1
 *  -------+--------------------+--------------
 *   B     | unsigned char      | 1
 *  -------+--------------------+--------------
 *   h     | short              | 2
 *  -------+--------------------+--------------
 *   H     | unsigned short     | 2
 *  -------+--------------------+--------------
 *   i     | int                | 4
 *  -------+--------------------+--------------
 *   I     | unsigned int       | 4
 *  -------+--------------------+--------------
 *   l     | long               | 4
 *  -------+--------------------+--------------
 *   L     | unsigned long      | 4
 *  -------+--------------------+--------------
 *   q     | long long          | 8
 *  -------+--------------------+--------------
 *   Q     | unsigned long long | 8
 *  -------+--------------------+--------------
 *   f     | float              | 4
 *  -------+--------------------+--------------
 *   d     | double             | 8
 *  -------+--------------------+--------------
 *   s     | char *             | Null-terminated string 
 *  -------+--------------------+--------------
 *   o     | char *             | blob data
 *  -------+--------------------+---------------
 *   t     | struct timeb       | sizeof(struct timeb) == 14
 *  --------------------------------------------
 *
 * Note. blob data and null-terminated string will add extern null character.
 *
 * +-----------------+--------+--------+...+--------+--------+...+--------+
 * | 4 byte length n | byte 0 | byte 1 |...|byte n-1|    0   |...|    0   |
 * +-----------------+--------+--------+...+--------+--------+...+--------+
 *                   |<-----------n bytes data----->|<------r bytes------>|
 *                   |<-----------n+r (where (n+r) mod 4 = 0)>----------->|
 *
 * A format character may be preceded by an integral repeat count.
 * For example, the format string '4h' means exactly the same as 'hhhh'.
 *
 * For the 's' format character:
 * [string length 4 byte][string content][0 ~ 3 zero]
 *
 * Example 1. pack/unpack int type value.
 *
 * char buf[BUFSIZ] = {0, };
 * int val = 0x12345678;
 * int oval;
 *
 * struct_pack(buf, "i", val);
 * struct_unpack(buf, "i", &oval);
 *
 * Example 2. pack/unpack a string.
 *
 * char buf[BUFSIZ] = {0, );
 * char *ostr, *str;
 *
 * struct_pack(buf, "!2s", "test", "packet");
 *
 * now, buf maybe like this:
 * 
 * OFFSET  HEX BYTES    ASCII  COMMENT
 * ------  ----------- ------ --------
 *  00     00 00 00 04  ....   length of first string
 *  04     74 65 73 74  test   first string data
 *  08     00 00 00 06  ....   length of second string
 *  12     70 61 63 6b  pack   second string data
 *  16     65 74 00 00  et..   .. and 2 zero-byte to fill   
 *
 * struct_unpack(buf, "!2s", &ostr, &str);
 *
 */
#ifndef _STRUCT_H_
#define _STRUCT_H_

#ifdef __cplusplus
extern "C" {
#endif

extern int struct_pack(unsigned char *buf, const char *fmt, ...);
extern int struct_pack_into(int offset, unsigned char *buf, const char *fmt, ...);
extern int struct_unpack(unsigned char *buf, const char *fmt, ...);
extern int struct_unpack_from(int offset, unsigned char *buf, const char *fmt, ...);
extern int struct_calcsize(const char *fmt);

#ifdef __cplusplus
}
#endif

#endif /* _STRUCT_H_ */

