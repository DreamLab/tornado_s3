import sys
import os
import logging
import uuid
import urllib.parse
import urllib.request, urllib.parse, urllib.error
import time

import tornado.options
import tornado.ioloop
import tornado.web
import tornado.httpclient as httpclient

from tornado_s3 import S3Bucket


class TestHandler(tornado.web.RequestHandler):
    def get(self):
        # send email notifaction
        """
        user_msg = EmailMessage()
        user_msg.subject = u"Test"
        user_msg.bodyHtml = "This is the test content."
        self.send("your@address.net", "user@address.net", user_msg)
        """
        pass

settings = {
    #"AmazonAccessKeyID": "00000000000000000000",
    #"AmazonSecretAccessKey": "0000000000000000000000000000000000000000",
    "debug": True,
}

application = tornado.web.Application([
    (r"/", TestHandler),
], **settings)

"""
if __name__ == "__main__":
    tornado.options.parse_command_line()
    application.listen(int(sys.argv[1]))
    tornado.ioloop.IOLoop.instance().start()
"""

s = S3Bucket("mybucket",
             access_key=settings["AmazonAccessKeyID"],
             secret_key=settings["AmazonSecretAccessKey"],
             base_url="http://s3-ap-southeast-1.amazonaws.com/mybucket")


def delete_callback(success):
    print(success)

def info_callback(i):
    print(i)
    s.delete(["my file 3","my file 4"], callback=delete_callback)

def get_callback(response):
    print(response.body)
    s.info("my file", callback=info_callback)

def list_callback(l):
    for (key, modify, etag, size) in l:
        print(key)
    s.get("my file", callback=get_callback)

def put_callback():
    print('upload success')
    print()
    s.listdir(limit=10, callback=list_callback)

#create file
s.put("my file", "my content", callback=put_callback)



if __name__ == "__main__":
    tornado.ioloop.IOLoop.instance().start()
