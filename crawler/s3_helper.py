import boto
import math, os, time
import uuid
from hashlib import md5
from boto.s3.key import Key
from filechunkio import FileChunkIO

BUCKET_NAME = "<>"


class Bucket(object):
    def __init__(self):
        self.client = boto.connect_s3()
        self.bucket = self.client.get_bucket(BUCKET_NAME)

    def uplaod(self, fileName):
        # Get the Key object of the bucket
        k = Key(self.bucket)
        # Crete a new key with id as the name of the file
        k.key = fileName
        f = open(fileName)
        # Upload the file
        result = k.set_contents_from_file(f)
        # result contains the size of the file uploaded
        print result

    def upload_large(self, source_path):
        source_size = os.stat(source_path).st_size
        source_key = md5(str(uuid.uuid4())).hexdigest() + ".mp4"
        print "source path %s" % source_path
        print "source key %s" % source_key

        mp = self.bucket.initiate_multipart_upload(source_key)
        chunk_size = 52428800
        chunk_count = int(math.ceil(source_size / float(chunk_size)))
        for i in range(chunk_count):
            offset = chunk_size * i
            bytes = min(chunk_size, source_size - offset)
            with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
                mp.upload_part_from_file(fp, part_num=i + 1)
        mp.complete_upload()

        k = Key(self.bucket)
        k.key = source_key
        k.set_remote_metadata({'iname': os.path.basename(source_path),
                               "iscale": str(source_size),
                               "itime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())) )
                               },
                              {}, True)
        remote_key = self.bucket.get_key(source_key)
        print "Key name", len(remote_key.metadata)


if __name__ == "__main__":
    bb = Bucket()
    bb.uplaod("__init__.py")
