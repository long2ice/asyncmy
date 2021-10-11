import binascii
import re
import struct
from io import BytesIO
from typing import Set, Union


class Gtid:
    @staticmethod
    def overlap(i1, i2):
        return i1[0] < i2[1] and i1[1] > i2[0]

    @staticmethod
    def contains(i1, i2):
        return i2[0] >= i1[0] and i2[1] <= i1[1]

    @staticmethod
    def parse_interval(interval):
        """
        We parse a human-generated string here. So our end value b
        is incremented to conform to the internal representation format.
        """
        m = re.search("^([0-9]+)(?:-([0-9]+))?$", interval)
        if not m:
            raise ValueError("GTID format is incorrect: %r" % (interval,))
        a = int(m.group(1))
        b = int(m.group(2) or a)
        return a, b + 1

    @staticmethod
    def parse(gtid: str):
        m = re.search(
            "^([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})" "((?::[0-9-]+)+)$",
            gtid,
        )
        if not m:
            raise ValueError("GTID format is incorrect: %r" % (gtid,))

        sid = m.group(1)
        intervals = m.group(2)

        intervals_parsed = [Gtid.parse_interval(x) for x in intervals.split(":")[1:]]

        return sid, intervals_parsed

    def _add_interval(self, itvl):
        """
        Use the internal representation format and add it
        to our intervals, merging if required.
        """
        new = []

        if itvl[0] > itvl[1]:
            raise Exception("Malformed interval %s" % (itvl,))

        if any(self.overlap(x, itvl) for x in self.intervals):
            raise Exception("Overlapping interval %s" % (itvl,))

        # Merge: arrange interval to fit existing set
        for existing in sorted(self.intervals):
            if itvl[0] == existing[1]:
                itvl = (existing[0], itvl[1])
                continue

            if itvl[1] == existing[0]:
                itvl = (itvl[0], existing[1])
                continue

            new.append(existing)

        self.intervals = sorted(new + [itvl])

    def __sub_interval(self, itvl):
        """Using the internal representation, remove an interval"""
        new = []

        if itvl[0] > itvl[1]:
            raise Exception("Malformed interval %s" % (itvl,))

        if not any(self.overlap(x, itvl) for x in self.intervals):
            # No raise
            return

        # Merge: arrange existing set around interval
        for existing in sorted(self.intervals):
            if self.overlap(existing, itvl):
                if existing[0] < itvl[0]:
                    new.append((existing[0], itvl[0]))
                if existing[1] > itvl[1]:
                    new.append((itvl[1], existing[1]))
            else:
                new.append(existing)

        self.intervals = new

    def __contains__(self, other):
        if other.sid != self.sid:
            return False

        return all(
            any(self.contains(me, them) for me in self.intervals) for them in other.intervals
        )

    def __init__(self, gtid: str, sid=None, intervals=None):
        if intervals is None:
            intervals = []
        if sid:
            intervals = intervals
        else:
            sid, intervals = Gtid.parse(gtid)

        self.sid = sid
        self.intervals = []
        for itvl in intervals:
            self._add_interval(itvl)

    def __add__(self, other):
        """Include the transactions of this gtid. Raise if the
        attempted merge has different SID"""
        if self.sid != other.sid:
            raise Exception("Attempt to merge different SID" "%s != %s" % (self.sid, other.sid))

        result = Gtid(str(self))

        for itvl in other.intervals:
            result._add_interval(itvl)

        return result

    def __sub__(self, other):
        """Remove intervals. Do not raise, if different SID simply
        ignore"""
        result = Gtid(str(self))
        if self.sid != other.sid:
            return result

        for itvl in other.intervals:
            result.__sub_interval(itvl)

        return result

    def __str__(self):
        """We represent the human value here - a single number
        for one transaction, or a closed interval (decrementing b)"""
        return "%s:%s" % (
            self.sid,
            ":".join(
                ("%d-%d" % (x[0], x[1] - 1)) if x[0] + 1 != x[1] else str(x[0])
                for x in self.intervals
            ),
        )

    def __repr__(self):
        return '<Gtid "%s">' % self

    @property
    def encoded_length(self):
        return (
            16
            + 8  # sid
            + 2  # n_intervals
            * 8  # stop/start
            * len(self.intervals)  # stop/start mark encoded as int64
        )

    def encode(self):
        buffer = b""
        # sid
        buffer += binascii.unhexlify(self.sid.replace("-", ""))
        # n_intervals
        buffer += struct.pack("<Q", len(self.intervals))

        for interval in self.intervals:
            # Start position
            buffer += struct.pack("<Q", interval[0])
            # Stop position
            buffer += struct.pack("<Q", interval[1])

        return buffer

    @classmethod
    def decode(cls, payload: BytesIO):
        sid = b""
        sid = sid + binascii.hexlify(payload.read(4))
        sid = sid + b"-"
        sid = sid + binascii.hexlify(payload.read(2))
        sid = sid + b"-"
        sid = sid + binascii.hexlify(payload.read(2))
        sid = sid + b"-"
        sid = sid + binascii.hexlify(payload.read(2))
        sid = sid + b"-"
        sid = sid + binascii.hexlify(payload.read(6))

        (n_intervals,) = struct.unpack("<Q", payload.read(8))
        intervals = []
        for i in range(0, n_intervals):
            start, end = struct.unpack("<QQ", payload.read(16))
            intervals.append((start, end - 1))

        return cls(
            "%s:%s"
            % (
                sid.decode("ascii"),
                ":".join(["%d-%d" % x for x in intervals]),
            )
        )

    def __eq__(self, other):
        if other.sid != self.sid:
            return False
        return self.intervals == other.intervals

    def __lt__(self, other):
        if other.sid != self.sid:
            return self.sid < other.sid
        return self.intervals < other.intervals

    def __le__(self, other):
        if other.sid != self.sid:
            return self.sid <= other.sid
        return self.intervals <= other.intervals

    def __gt__(self, other):
        if other.sid != self.sid:
            return self.sid > other.sid
        return self.intervals > other.intervals

    def __ge__(self, other):
        if other.sid != self.sid:
            return self.sid >= other.sid
        return self.intervals >= other.intervals


class GtidSet:
    def __init__(self, gtid_set: Set[Gtid]):
        self._gtid_set = gtid_set

    def merge_gtid(self, gtid: Gtid):
        new_gtid_set = set()
        for existing in self._gtid_set:
            if existing.sid == gtid.sid:
                new_gtid_set.add(existing + gtid)
            else:
                new_gtid_set.add(existing)
        if gtid.sid not in (x.sid for x in new_gtid_set):
            new_gtid_set.add(gtid)
        self._gtid_set = new_gtid_set

    def __contains__(self, other: Union[Gtid, "GtidSet"]):
        if isinstance(other, GtidSet):
            return all(other_gtid in self._gtid_set for other_gtid in other._gtid_set)
        if isinstance(other, Gtid):
            return any(other in x for x in self._gtid_set)
        raise NotImplementedError

    def __add__(self, other: Union[Gtid, "GtidSet"]):
        if isinstance(other, Gtid):
            new = GtidSet(self._gtid_set)
            new.merge_gtid(other)
            return new

        if isinstance(other, GtidSet):
            new = GtidSet(self._gtid_set)
            for gtid in other._gtid_set:
                new.merge_gtid(gtid)
            return new

        raise NotImplementedError

    def __str__(self):
        return ",".join(str(x) for x in self._gtid_set)

    def __repr__(self):
        return "<GtidSet %r>" % self._gtid_set

    @property
    def encoded_length(self):
        return 8 + sum(x.encoded_length for x in self._gtid_set)  # n_sids

    def encoded(self):
        return b"" + (
            struct.pack("<Q", len(self._gtid_set)) + b"".join(x.encode() for x in self._gtid_set)
        )

    encode = encoded

    @classmethod
    def decode(cls, payload: BytesIO):
        (n_sid,) = struct.unpack("<Q", payload.read(8))
        return cls(set(Gtid.decode(payload) for _ in range(0, n_sid)))

    def __eq__(self, other: "GtidSet"):  # type: ignore[override]
        return self._gtid_set == other._gtid_set
