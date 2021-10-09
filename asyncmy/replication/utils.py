from asyncmy.structs import B


def byte2int(b):
    if isinstance(b, int):
        return b
    else:
        return B.unpack(b)[0]


def int2byte(i: int):
    return B.pack(i)
