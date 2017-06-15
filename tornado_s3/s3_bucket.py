"""Bucket manipulation"""

import datetime
import time
import warnings
from contextlib import contextmanager
from functools import partial

import tornado.httpclient as httpclient
from tornado import gen

from tornado_s3.exceptions.key_exceptions import KeyNotFound
from .s3_listing import S3Listing
from .s3_request import S3Request
from .utils import (metadata_headers, aws_md5, aws_urlquote, guess_mimetype, info_dict, expire2datetime)

amazon_s3_domain = "s3.amazonaws.com"


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
        # self.opener = self.build_opener()
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

    def __getitem__(self, name):
        return self.get(name)

    def __delitem__(self, name):
        return self.delete(name)

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

    @gen.coroutine
    def send(self, s3req, callback=None):
        s3req.sign(self)
        req = s3req.urllib(self)
        try:
            http_client = httpclient.AsyncHTTPClient()
            result = yield http_client.fetch(req, callback)
            return result
        except httpclient.HTTPError as e:
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

    @gen.coroutine
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
        result = yield self.send(s3req, partial(self._put, callback=callback))
        return result

    def _delete(self, response, callback):
        success = 200 <= response.code < 300
        if callback: callback(success)

    def delete(self, key, callback=None):
        try:
            self.send(self.request(method="DELETE", key=key), partial(self._delete, callback=callback))
        except KeyNotFound as e:
            e.fp.close()

    def _listing(self, response, result, args, callback):
        listing = S3Listing.parse(response.buffer)
        result.extend(listing)

        if listing.truncated:
            args["marker"] = listing.next_marker
            self.send(self.request(args=args), partial(self._listing, result=result, args=args, callback=callback))
        else:
            if callback: callback(result)

    @gen.coroutine
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

        result = yield self.send(self.request(args=args),
                                 partial(self._listing, result=[], args=args, callback=callback))
        return result

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
