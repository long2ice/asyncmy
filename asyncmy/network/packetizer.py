from asysocks.unicomm.common.packetizers import Packetizer


class MySQLPacketizer(Packetizer):
    """Frames the raw byte stream into MySQL wire packets.

    A MySQL packet is a 3-byte little-endian payload length, a 1-byte
    sequence id, followed by the payload. This packetizer yields each
    complete *physical* packet (header included) so the connection layer
    can keep validating sequence ids and reassembling payloads larger than
    16 MiB, exactly as the classic driver did.
    """

    def __init__(self, buffer_size: int = 65535):
        Packetizer.__init__(self, buffer_size)
        self.in_buffer = b""

    def flush_buffer(self):
        buff = self.in_buffer
        self.in_buffer = b""
        return buff

    def process_buffer(self):
        while True:
            if len(self.in_buffer) < 4:
                return
            payload_length = int.from_bytes(self.in_buffer[0:3], byteorder="little", signed=False)
            total = 4 + payload_length
            if len(self.in_buffer) < total:
                return
            frame = self.in_buffer[:total]
            self.in_buffer = self.in_buffer[total:]
            yield frame

    async def data_out(self, data):
        # The connection layer already prepends the MySQL header (length +
        # sequence id) so nothing to frame on the way out.
        yield data

    async def data_in(self, data):
        if data is None:
            for frame in self.process_buffer():
                yield frame
            return
        self.in_buffer += data
        for frame in self.process_buffer():
            yield frame
