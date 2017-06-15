from .s3_error import S3Error


class KeyNotFound(S3Error, KeyError):
    @property
    def key(self): return self.extra.get("key")
