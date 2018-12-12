import boto
import json
from boto.sqs.message import Message, RawMessage


class SQS(object):
    def __init__(self):
        self.conn = boto.sqs.connect_to_region("us-east-1")
        self.queue = self.conn.get_queue('<>')
        #self.queue.set_message_class(RawMessage)

    def send_message(self, message):
        m = Message()
        m.set_body(json.dumps(message))
        return self.queue.write(m)
