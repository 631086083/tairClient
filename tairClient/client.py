import datetime
from redis.client import Redis, Pipeline

from redis._compat import iteritems

from redis.client import list_or_args
from redis.exceptions import (
    DataError,
)


class Client(Redis):
    """
    tairClient-py is a package that gives developers easy access to tair or tairModules. The package extends
    redis-py's interface with Tair's API.
    """

    TAIRHASH_EXHSET = "EXHSET"
    TAIRHASH_EXHMSET = "EXHMSET"
    TAIRHASH_EXHPEXPIREAT = "EXHPEXPIREAT"
    TAIRHASH_EXHPEXPIRE = "EXHPEXPIRE"
    TAIRHASH_EXHEXPIREAT = "EXHEXPIREAT"
    TAIRHASH_EXHEXPIRE = "EXHEXPIRE"
    TAIRHASH_EXHPTTL = "EXHPTTL"
    TAIRHASH_EXHTTL = "EXHTTL"
    TAIRHASH_EXHVER = "EXHVER"
    TAIRHASH_EXHSETVER = "EXHSETVER"
    TAIRHASH_EXHINCRBY = "EXHINCRBY"
    TAIRHASH_EXHINCRBYFLOAT = "EXHINCRBYFLOAT"
    TAIRHASH_EXHGET = "EXHGET"
    TAIRHASH_EXHGETWITHVER = "EXHGETWITHVER"
    TAIRHASH_EXHMGET = "EXHMGET"
    TAIRHASH_EXHMGETWITHVER = "EXHMGETWITHVER"
    TAIRHASH_EXHDEL = "EXHDEL"
    TAIRHASH_EXHLEN = "EXHLEN"
    TAIRHASH_EXHEXISTS = "EXHEXISTS"
    TAIRHASH_EXHSTRLEN = "EXHSTRLEN"
    TAIRHASH_EXHKEYS = "EXHKEYS"
    TAIRHASH_EXHVALS = "EXHVALS"
    TAIRHASH_EXHGETALL = "EXHGETALL"
    TAIRHASH_EXHSCAN = "EXHSCAN"
    TAIRHASH_DEL = "DEL"

    def __init__(self, *args, **kwargs):
        """
        Creates a new tairHash client.
        """
        Redis.__init__(self, *args, **kwargs)

    @staticmethod
    def appendExpire(pieces, ex, exat, px, pxat):
        if ex is not None:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = int(ex.total_seconds())
            pieces.append(ex)
        if exat is not None:
            pieces.append('EXAT')
            if isinstance(exat, datetime.timedelta):
                exat = int(exat.total_seconds())
            pieces.append(exat)
        if px is not None:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                px = int(px.total_seconds() * 1000)
            pieces.append(px)
        if pxat is not None:
            pieces.append('PXAT')
            if isinstance(pxat, datetime.timedelta):
                pxat = int(pxat.total_seconds() * 1000)
            pieces.append(pxat)

    @staticmethod
    def appendExists(pieces, nx, xx):
        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')

    @staticmethod
    def appendVer(pieces, ver):
        if ver is not None:
            pieces.extend(['VER', ver])

    @staticmethod
    def appendAbs(pieces, abs):
        if abs is not None:
            pieces.extend(['Abs', abs])

    @staticmethod
    def appendNoActive(pieces, noactive):
        if noactive:
            pieces.append('NOACTIVE')

    @staticmethod
    def appendMinVal(pieces, minval):
        if minval is not None:
            pieces.extend(['Min', minval])

    @staticmethod
    def appendMaxVal(pieces, maxval):
        if maxval is not None:
            pieces.extend(['Max', maxval])

    def exhset(self, name, field, value, ex=None, exat=None, px=None, pxat=None,
               nx=False, xx=False, ver=None, abs=None, noactive=False):
        """
        insert a field into TairHash specified by key. If TairHash does not exist, one will be created automatically,
        and if the field already exists, its value will be overwritten.
        :param name: same as hash's key
        :param field: same as hash's field
        :param value: same as hash's value
        :param ex: set an expire flag on key ``name`` for ``ex`` seconds.
        :param exat: Set the absolute expiration time for a field in milliseconds
        :param px: set an expire flag on key ``name`` for ``ex`` milliseconds.
        :param pxat: set the absolute expiration time for a field in seconds
        :param nx: if field does not exist, it will be created automatically
        :param xx: only if field already exist, it will be set to replace old one
        :param ver: set the field's version
        :param abs: set the absolute field's version
        :param noactive: Setting NOACTIVE means that the field does not use active expiration strategy
        :return:
        """

        pieces = [name, field, value]

        self.appendExpire(pieces, ex, exat, px, pxat)
        self.appendExists(pieces, nx, xx)
        self.appendVer(pieces, ver)
        self.appendAbs(pieces, abs)
        self.appendNoActive(pieces, noactive)
        return self.execute_command(self.TAIRHASH_EXHSET, *pieces)

    def exhget(self, name, field):
        """
        get the value of a field in the specified TairHash.
        :param name: same as hash's key
        :param field: same as hash's field
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHGET, name, field)

    def exhmset(self, name, mapping):
        """
        at the same time, insert multiple fields into the TairHash specified by the key. If TairHash does not exist,
        one will be created automatically, and if the field already exists, its value will be overwritten.
        :param name: same as hash's key
        :param mapping: field-value pairs
        :return:
        """
        if not mapping:
            raise DataError("'exhmset' with 'mapping' of length 0")
        pieces = []
        for pair in iteritems(mapping):
            pieces.extend(pair)
        return self.execute_command(self.TAIRHASH_EXHMSET, name, *pieces)

    def exhpexpireat(self, name, field, pxat, ver=None, abs=None, noactive=False):
        """
        set the absolute expiration time for a field in the TairHash specified by the key, accurate to the millisecond.
        :param name: same as hash's key
        :param field: same as hash's field
        :param pxat: set the absolute expiration time for a field in seconds
        :param ver: set the field's version
        :param abs: set the absolute field's version
        :param noactive: Setting NOACTIVE means that the field does not use active expiration strategy
        :return:
        """
        pieces = [name, field, pxat]
        self.appendVer(pieces, ver)
        self.appendAbs(pieces, abs)
        self.appendNoActive(pieces, noactive)
        return self.execute_command(self.TAIRHASH_EXHPEXPIREAT, *pieces)

    def exhpexpire(self, name, field, px, ver=None, abs=None, noactive=False):
        """
        set the relative expiration time for a field in the TairHash specified by the key, in milliseconds.
        :param name: same as hash's key
        :param field: same as hash's field
        :param px: set an expire flag on key ``name`` for ``ex`` milliseconds.
        :param ver: set the field's version
        :param abs: set the absolute field's version
        :param noactive: Setting NOACTIVE means that the field does not use active expiration strategy
        :return:
        """
        pieces = [name, field, px]
        self.appendVer(pieces, ver)
        self.appendAbs(pieces, abs)
        self.appendNoActive(pieces, noactive)
        return self.execute_command(self.TAIRHASH_EXHPEXPIRE, *pieces)

    def exhexpireat(self, name, field, exat, ver=None, abs=None, noactive=False):
        """
        set the absolute expiration time for a field in the TairHash specified by the key, accurate to the second.
        :param name: same as hash's key
        :param field: same as hash's field
        :param exat: set the absolute expiration time for a field in seconds
        :param ver: set the field's version
        :param abs: set the absolute field's version
        :param noactive: Setting NOACTIVE means that the field does not use active expiration strategy
        :return:
        """
        pieces = [name, field, exat]
        self.appendVer(pieces, ver)
        self.appendAbs(pieces, abs)
        self.appendNoActive(pieces, noactive)
        return self.execute_command(self.TAIRHASH_EXHEXPIREAT, *pieces)

    def exhexpire(self, name, field, ex, ver=None, abs=None, noactive=False):
        """
        set the relative expiration time for a field in the TairHash specified by the key, in seconds.
        :param name: same as hash's key
        :param field: same as hash's field
        :param ex: set an expire flag on key ``name`` for ``ex`` seconds.
        :param ver: set the field's version
        :param abs: set the absolute field's version
        :param noactive: Setting NOACTIVE means that the field does not use active expiration strategy
        :return:
        """
        pieces = [name, field, ex]
        self.appendVer(pieces, ver)
        self.appendAbs(pieces, abs)
        self.appendNoActive(pieces, noactive)
        return self.execute_command(self.TAIRHASH_EXHEXPIRE, *pieces)

    def exhpttl(self, name, field):
        """
        check the expiration time of a field in TairHash specified by key, and the result is accurate to milliseconds.
        :param name: same as hash's key
        :param field: same as hash's field
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHPTTL, name, field)

    def exhttl(self, name, field):
        """
        check the expiration time of a field in TairHash specified by key, and the result is accurate to seconds.
        :param name: same as hash's key
        :param field: same as hash's field
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHTTL, name, field)

    def exhver(self, name, field):
        """
        check the version number of a field in TairHash specified by key.
        :param name: same as hash's key
        :param field: same as hash's field
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHVER, name, field)

    def exhsetver(self, name, field, ver):
        """
        set the version number of a field in TairHash specified by key.
        :param name: same as hash's key
        :param field: same as hash's field
        :param ver: set the field's version
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHSETVER, name, field, ver)

    def exhincrby(self, name, field, digit, ex=None, exat=None, px=None, pxat=None,
                  nx=False, xx=False, ver=None, abs=None, minval=None, maxval=None):
        """
        Increase the value of a field in TairHash specified by key by floating, which is a float number.
        If TairHash does not exist, it will automatically create a new one. If the specified field does
        not exist, insert the field before adding and set its value to 0. This command will trigger the
        passive elimination check of the field.
        :param name: same as hash's key
        :param field: same as hash's field
        :param digit: int value which will be incr for the field
        :param ex: set an expire flag on key ``name`` for ``ex`` seconds.
        :param exat: Set the absolute expiration time for a field in milliseconds
        :param px: set an expire flag on key ``name`` for ``ex`` milliseconds.
        :param pxat: set the absolute expiration time for a field in seconds
        :param nx: if field does not exist, it will be created automatically
        :param xx: only if field already exist, it will be set to replace old one
        :param ver: set the field's version
        :param abs: set the absolute field's version
        :param minval: The minimum value of value. If it is less than this value, an exception will be prompted.
        :param maxval: The maximum value of value. If it is more than this value, an exception will be prompted.
        :return:
        """
        pieces = [name, field, digit]
        self.appendExpire(pieces, ex, exat, px, pxat)
        self.appendExists(pieces, nx, xx)
        self.appendVer(pieces, ver)
        self.appendAbs(pieces, abs)
        self.appendMinVal(pieces, minval)
        self.appendMaxVal(pieces, maxval)
        return self.execute_command(self.TAIRHASH_EXHINCRBY, *pieces)

    def exhincrbyfloat(self, name, field, floating, ex=None, exat=None, px=None, pxat=None,
                       nx=False, xx=False, ver=None, abs=None, minval=None, maxval=None):
        """
        Increase the value of a field in TairHash specified by key by floating, which is a float number.
        If TairHash does not exist, it will automatically create a new one. If the specified field does
        not exist, insert the field before adding and set its value to 0. This command will trigger the
        passive elimination check of the field.
        :param name: same as hash's key
        :param field: same as hash's field
        :param floating: float value which will be incr for the field
        :param ex: set an expire flag on key ``name`` for ``ex`` seconds.
        :param exat: Set the absolute expiration time for a field in milliseconds
        :param px: set an expire flag on key ``name`` for ``ex`` milliseconds.
        :param pxat: set the absolute expiration time for a field in seconds
        :param nx: if field does not exist, it will be created automatically
        :param xx: only if field already exist, it will be set to replace old one
        :param ver: set the field's version
        :param abs: set the absolute field's version
        :param minval: The minimum value of value. If it is less than this value, an exception will be prompted.
        :param maxval: The maximum value of value. If it is more than this value, an exception will be prompted.
        :return:
        """
        pieces = [name, field, floating]
        self.appendExpire(pieces, ex, exat, px, pxat)
        self.appendExists(pieces, nx, xx)
        self.appendVer(pieces, ver)
        self.appendAbs(pieces, abs)
        self.appendMinVal(pieces, minval)
        self.appendMaxVal(pieces, maxval)
        return self.execute_command(self.TAIRHASH_EXHINCRBYFLOAT, *pieces)

    def exhgetwithver(self, name, field):
        """
        At the same time, obtain the value and version of a field in TaiHash specified by the key.
        :param name: same as hash's key
        :param field: same as hash's field
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHGETWITHVER, name, field)

    def exhmget(self, name, field, *args):
        """
        At the same time, obtain the value of multiple fields of TaiHash specified by key.
        :param name: same as hash's key
        :param field: same as hash's field
        :param args: one or more field can be transformed by *args
        :return:
        """
        args = list_or_args(field, args)
        return self.execute_command(self.TAIRHASH_EXHMGET, name, *args)

    def exhmgetwithver(self, name, field, *args):
        """
        At the same time obtain the value and version of multiple TairHash fields specified by the key.
        :param name: same as hash's key
        :param field: same as hash's field
        :param args: one or more field can be transformed by *args
        :return:
        """
        args = list_or_args(field, args)
        return self.execute_command(self.TAIRHASH_EXHMGETWITHVER, name, *args)

    def exhdel(self, name, field, *args):
        """
        Delete a field in TairHash specified by key.
        :param name: same as hash's key
        :param field: same as hash's field
        :param args: one or more field can be transformed by *args
        :return:
        """
        args = list_or_args(field, args)
        return self.execute_command(self.TAIRHASH_EXHDEL, name, *args)

    def exhlen(self, name, noexp=False):
        """
        Get the number of fields in TaiHash specified by key. Maybe fields that have expired and not been deleted.
        :param name: same as hash's key
        :param noexp: If you only want to return the current number of fields that have not expired, you can set the
                      noexp option in the command
        :return:
        """
        if noexp:
            return self.execute_command(self.TAIRHASH_EXHLEN, name, 'NOEXP')
        else:
            return self.execute_command(self.TAIRHASH_EXHLEN, name)

    def exhexists(self, name, field):
        """
        Query whether there is a corresponding field in TairHash specified by key.
        :param name: same as hash's key
        :param field: same as hash's field
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHEXISTS, name, field)

    def exhserlen(self, name, field):
        """
        Get the length of the value of a field in TairHash specified by key.
        :param name: same as hash's key
        :param field: same as hash's field
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHSTRLEN, name, field)

    def exhkeys(self, name):
        """
        Get all the fields in TairHash specified by key.
        :param name: same as hash's key
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHKEYS, name)

    def exhvals(self, name):
        """
        Get the value of all fields in TairHash specified by key.
        :param name: same as hash's key
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHVALS, name)

    def exhgetall(self, name):
        """
        Get the all fields with its' values in TairHash specified by key.
        :param name: same as hash's key
        :return:
        """
        return self.execute_command(self.TAIRHASH_EXHGETALL, name)

    def exhscan(self, name, cursor, match=None, count=None):
        """
        Scan the TairHash specified by the key.
        :param name: same as hash's key
        :param cursor: the cursor of scan
        :param match: Used to filter scan results.
        :param count: The number of values returned each time
        :return:
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend(['MATCH', match])
        if count is not None:
            pieces.extend(['COUNT', count])
        return self.execute_command(self.TAIRHASH_EXHSCAN, *pieces)

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution.
        """
        p = Pipeline(
            connection_pool=self.connection_pool,
            response_callbacks=self.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint)
        return p


class Pipeline(Pipeline, Client):

    def __init__(self, connection_pool, response_callbacks, transaction, shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint

        self.watching = False
        self.reset()
