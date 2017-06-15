import urllib
import http



class S3Error(Exception):
    fp = None

    def __init__(self, message, **kwds):
        self.args = message, kwds.copy()
        self.msg, self.extra = self.args

    def __str__(self):
        rv = self.msg
        if self.extra:
            rv += " ("
            rv += ", ".join("%s=%r" % i for i in self.extra.items())
            rv += ")"
        return rv

    @classmethod
    def from_urllib(cls, e, **extra):
        """Try to read the real error from AWS."""
        self = cls("HTTP error", **extra)
        for attr in ("reason", "code", "filename"):
            if attr not in extra and hasattr(e, attr):
                self.extra[attr] = getattr(e, attr)
        self.fp = getattr(e, "fp", None)
        if self.fp:
            # The except clause is to avoid a bug in urllib2 which has it read
            # as in chunked mode, but S3 gives an empty reply.
            try:
                self.data = data = self.fp.read()
            except (http.client.HTTPException, urllib.error.URLError) as e:
                self.extra["read_error"] = e
            else:
                data = data.decode("utf-8")
                begin, end = data.find("<Message>"), data.find("</Message>")
                if min(begin, end) >= 0:
                    self.msg = data[begin + 9:end]
        return self

    @property
    def code(self): return self.extra.get("code")

