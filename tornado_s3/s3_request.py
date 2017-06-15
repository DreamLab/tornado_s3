from .utils import aws_md5, rfc822_fmtdate, _amz_canonicalize, aws_urlquote

import tornado.httpclient as httpclient

import hashlib
import hmac

from base64 import b64encode
from urllib.parse import quote_plus


class S3Request(object):
    # urllib_request_cls = AnyMethodRequest
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
        """Sign the request with credentials *cred*."""
        desc = self.descriptor()
        key = cred.secret_key.encode("utf-8")
        hasher = hmac.new(key, desc.encode("utf-8"), hashlib.sha1)
        sign = b64encode(hasher.digest()).decode()
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
