"""Bucket manipulation"""



import time
import hmac
import hashlib
import http.client
import urllib.request, urllib.error, urllib.parse
import datetime
import warnings
from xml.etree import cElementTree as ElementTree
from contextlib import contextmanager
from urllib.parse import quote_plus
from base64 import b64encode
from cgi import escape
from functools import partial

from .utils import (_amz_canonicalize, metadata_headers, rfc822_fmtdate, _iso8601_dt,
                    aws_md5, aws_urlquote, guess_mimetype, info_dict, expire2datetime)

import tornado.httpclient as httpclient


amazon_s3_domain = "s3.amazonaws.com"
amazon_s3_ns_url = "http://%s/doc/2006-03-01/" % amazon_s3_domain

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

class KeyNotFound(S3Error, KeyError):
    @property
    def key(self): return self.extra.get("key")

class S3Request(object):
    #urllib_request_cls = AnyMethodRequest
    urllib_request_cls = httpclient.HTTPRequest

    def __init__(self, bucket=None, key=None, method="GET", headers={},
                 args=None, data=None, subresource=None):
        headers = headers.copy()
        if data and "Content-MD5" not in headers:
            headers["Content-MD5"] = aws_md5(data)
        if "Date" not in headers:
            headers["Date"] = rfc822_fmtdate()
        if hasattr(bucket, "name"):
            bucket = bucket.name
        self.bucket = bucket
        self.key = key
        self.method = method
        self.headers = headers
        self.args = args
        self.data = data
        self.subresource = subresource

    def __str__(self):
        return "<S3 %s request bucket %r key %r>" % (self.method, self.bucket, self.key)

    def descriptor(self):
        # The signature descriptor is detalied in the developer's PDF on p. 65.
        lines = (self.method,
                 self.headers.get("Content-MD5", ""),
                 self.headers.get("Content-Type", ""),
                 self.headers.get("Date", ""))
        preamb = "\n".join(str(line) for line in lines) + "\n"
        headers = _amz_canonicalize(self.headers)
        res = self.canonical_resource
        return "".join((preamb, headers, res))

    @property
    def canonical_resource(self):
        res = "/%s/" % aws_urlquote(self.bucket)
        if self.key:
            res += aws_urlquote(self.key)
        if self.subresource:
            res += "?" + aws_urlquote(self.subresource)
        return res

    def sign(self, cred):
        "Sign the request with credentials *cred*."
        desc = self.descriptor()
        key = cred.secret_key.encode("utf-8")
        hasher = hmac.new(key, desc.encode("utf-8"), hashlib.sha1)
        sign = b64encode(hasher.digest())
        self.headers["Authorization"] = "AWS %s:%s" % (cred.access_key, sign)
        return sign

    def urllib(self, bucket):
        return self.urllib_request_cls(self.url(bucket.base_url), method=self.method,
                                       body=self.data, headers=self.headers)

    def url(self, base_url, arg_sep="&"):
        url = base_url + "/"
        if self.key:
            url += aws_urlquote(self.key)
        if self.subresource or self.args:
            ps = []
            if self.subresource:
                ps.append(self.subresource)
            if self.args:
                args = self.args
                if hasattr(args, "iteritems"):
                    args = iter(args.items())
                args = ((quote_plus(k), quote_plus(v)) for k, v in args.items())
                args = arg_sep.join("%s=%s" % i for i in args)
                ps.append(args)
            url += "?" + "&".join(ps)
        return url

class S3File(str):
    def __new__(cls, value, **kwds):
        return super(S3File, cls).__new__(cls, value)

    def __init__(self, value, **kwds):
        kwds["data"] = value
        self.kwds = kwds

    def put_into(self, bucket, key):
        return bucket.put(key, **self.kwds)

class S3Listing(object):
    """Representation of a single pageful of S3 bucket listing data."""

    truncated = None

    def __init__(self, etree):
        # TODO Use SAX - processes XML before downloading entire response
        root = etree.getroot()
        expect_tag = self._mktag("ListBucketResult")
        if root.tag != expect_tag:
            raise ValueError("root tag mismatch, wanted %r but got %r"
                             % (expect_tag, root.tag))
        self.etree = etree
        trunc_text = root.findtext(self._mktag("IsTruncated"))
        self.truncated = {"true": True, "false": False}[trunc_text]

    def __iter__(self):
        root = self.etree.getroot()
        for entry in root.findall(self._mktag("Contents")):
            item = self._el2item(entry)
            yield item
            self.next_marker = item[0]

    @classmethod
    def parse(cls, resp):
        return cls(ElementTree.parse(resp))

    def _mktag(self, name):
        return "{%s}%s" % (amazon_s3_ns_url, name)

    def _el2item(self, el):
        get = lambda tag: el.findtext(self._mktag(tag))
        key = get("Key")
        modify = _iso8601_dt(get("LastModified"))
        etag = get("ETag")
        size = int(get("Size"))
        return (key, modify, etag, size)

class S3Bucket(object):
    default_encoding = "utf-8"
    n_retries = 10

    def __init__(self, name, access_key=None, secret_key=None,
                 base_url=None, timeout=None, secure=False):
        scheme = ("http", "https")[int(bool(secure))]
        if not base_url:
            base_url = "%s://%s/%s" % (scheme, amazon_s3_domain, aws_urlquote(name))
        elif secure is not None:
            if not base_url.startswith(scheme + "://"):
                raise ValueError("secure=%r, url must use %s"
                                 % (secure, scheme))
        #self.opener = self.build_opener()
        self.name = name
        self.access_key = access_key
        self.secret_key = secret_key
        self.base_url = base_url
        self.timeout = timeout

    def __str__(self):
        return "<%s %s at %r>" % (self.__class__.__name__, self.name, self.base_url)

    def __repr__(self):
        return self.__class__.__name__ + "(%r, access_key=%r, base_url=%r)" % (
            self.name, self.access_key, self.base_url)

    def __getitem__(self, name): return self.get(name)
    def __delitem__(self, name): return self.delete(name)
    def __setitem__(self, name, value):
        if hasattr(value, "put_into"):
            return value.put_into(self, name)
        else:
            return self.put(name, value)
    def __contains__(self, name):
        try:
            self.info(name)
        except KeyError:
            return False
        else:
            return True

    @contextmanager
    def timeout_disabled(self):
        (prev_timeout, self.timeout) = (self.timeout, None)
        try:
            yield
        finally:
            self.timeout = prev_timeout

    def request(self, *a, **k):
        k.setdefault("bucket", self.name)
        return S3Request(*a, **k)

    def send(self, s3req, callback=None):
        s3req.sign(self)
        req = s3req.urllib(self)
        try:
            http_client = httpclient.AsyncHTTPClient()
            http_client.fetch(req, callback)

        except (httpclient.HTTPError) as e:
            pass

    def _get(self, response, callback):
        response.s3_info = info_dict(dict(response.headers))
        if callback: callback(response)

    def get(self, key, callback=None):
        self.send(self.request(key=key), partial(self._get, callback=callback))

    def _info(self, response, callback):
        rv = info_dict(dict(response.headers))
        if callback: callback(rv)

    def info(self, key, callback=None):
        self.send(self.request(method="HEAD", key=key), partial(self._info, callback=callback))

    def _put(self, response, callback):
        if callback: callback()

    def put(self, key, data=None, acl=None, metadata={}, mimetype=None,
            transformer=None, headers={}, callback=None):
        if isinstance(data, str):
            data = data.encode(self.default_encoding)
        headers = headers.copy()
        if mimetype:
            headers["Content-Type"] = str(mimetype)
        elif "Content-Type" not in headers:
            headers["Content-Type"] = guess_mimetype(key)
        headers.update(metadata_headers(metadata))
        if acl: headers["X-AMZ-ACL"] = acl
        if transformer: data = transformer(headers, data)
        if "Content-Length" not in headers:
            headers["Content-Length"] = str(len(data))
        if "Content-MD5" not in headers:
            headers["Content-MD5"] = aws_md5(data)

        s3req = self.request(method="PUT", key=key, data=data, headers=headers)
        self.send(s3req, partial(self._put, callback=callback))

    def _delete(self, response, callback):
        success = 200 <= response.code < 300
        if callback: callback(success)

    def delete(self, keys, callback=None):
        assert isinstance(keys, list)

        n_keys = len(keys)
        if not keys:
            raise TypeError("required one key at least")

        if n_keys == 1:
            # In <=py25, urllib2 raises an exception for HTTP 204, and later
            # does not, so treat errors and non-errors as equals.
            try:
                self.send(self.request(method="DELETE", key=keys[0]), partial(self._delete, callback=callback))
            except KeyNotFound as e:
                e.fp.close()
        else:
            if n_keys > 1000:
                raise ValueError("cannot delete more than 1000 keys at a time")
            fmt = "<Object><Key>%s</Key></Object>"
            body = "".join(fmt % escape(k) for k in keys)
            data = ('<?xml version="1.0" encoding="UTF-8"?><Delete>'
                    "<Quiet>true</Quiet>%s</Delete>") % body
            headers = {"Content-Type": "multipart/form-data"}
            self.send(self.request(method="POST", data=data,
                                   headers=headers, subresource="delete"), partial(self._delete, callback=callback))

    def _listing(self, response, result, args, callback):
        listing = S3Listing.parse(response.buffer)
        result.extend(listing)

        if listing.truncated:
            args["marker"] = listing.next_marker
            self.send(self.request(args=args), partial(self._listing, result=result, args=args, callback=callback))
        else:
            if callback: callback(result)

    def listdir(self, prefix=None, marker=None, limit=None, delimiter=None, callback=None):
        """List bucket contents.

        Yields tuples of (key, modified, etag, size).

        *prefix*, if given, predicates `key.startswith(prefix)`.
        *marker*, if given, predicates `key > marker`, lexicographically.
        *limit*, if given, predicates `len(keys) <= limit`.

        *key* will include the *prefix* if any is given.

        .. note:: This method can make several requests to S3 if the listing is
                  very long.
        """
        m = (("prefix", prefix),
             ("marker", marker),
             ("max-keys", limit),
             ("delimiter", delimiter))
        args = dict((str(k), str(v)) for (k, v) in m if v is not None)

        self.send(self.request(args=args), partial(self._listing, result=[], args=args, callback=callback))

    def make_url(self, key, args=None, arg_sep=";"):
        s3req = self.request(key=key, args=args)
        return s3req.url(self.base_url, arg_sep=arg_sep)

    def make_url_authed(self, key, expire=datetime.timedelta(minutes=5)):
        """Produce an authenticated URL for S3 object *key*.

        *expire* is a delta or a datetime on which the authenticated URL
        expires. It defaults to five minutes, and accepts a timedelta, an
        integer delta in seconds, or a datetime.

        To generate an unauthenticated URL for a key, see `B.make_url`.
        """
        # NOTE There is a usecase for having a headers argument to this
        # function - Amazon S3 will validate the X-AMZ-* headers of the GET
        # request, and so for the browser to send such a header, it would have
        # to be listed in the signature description.
        expire = expire2datetime(expire)
        expire = time.mktime(expire.timetuple()[:9])
        expire = str(int(expire))
        s3req = self.request(key=key, headers={"Date": expire})
        sign = s3req.sign(self)
        s3req.args = (("AWSAccessKeyId", self.access_key),
                      ("Expires", expire),
                      ("Signature", sign))
        return s3req.url(self.base_url, arg_sep="&")

    def url_for(self, key, authenticated=False,
                expire=datetime.timedelta(minutes=5)):
        msg = "use %s instead of url_for(authenticated=%r)"
        dep_cls = DeprecationWarning
        if authenticated:
            warnings.warn(dep_cls(msg % ("make_url_authed", True)))
            return self.make_url_authed(key, expire=expire)
        else:
            warnings.warn(dep_cls(msg % ("make_url", False)))
            return self.make_url(key)
