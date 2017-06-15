from xml.etree import cElementTree as ElementTree
from .utils import _iso8601_dt

amazon_s3_domain = "s3.amazonaws.com"
amazon_s3_ns_url = "http://%s/doc/2006-03-01/" % amazon_s3_domain


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
        return key, modify, etag, size
