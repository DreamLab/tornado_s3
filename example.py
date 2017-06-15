import tornado.web
from tornado import gen

from tornado_s3 import S3Bucket

settings = {
    "AmazonAccessKeyID": "XXXXXXXXXXXXXXXX",
    "AmazonSecretAccessKey": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "debug": True,
}

s = S3Bucket("$BUCKET",
             access_key=settings["AmazonAccessKeyID"],
             secret_key=settings["AmazonSecretAccessKey"],
             base_url="http://$URL/$BUCKET")

filename = 'myfile 3'


@gen.coroutine
def do_something():
    print("going to put a file")
    res = yield s.put(filename, "my content")
    print("response?", res)
    list_dir = yield s.listdir(limit=10)
    print(list_dir)


@gen.coroutine
def minute_loop():
    while True:
        yield do_something()
        yield gen.sleep(60)


if __name__ == "__main__":
    tornado.ioloop.IOLoop.current().spawn_callback(minute_loop)
    print("after start")
    tornado.ioloop.IOLoop.instance().start()
