import pika

from django.core.management.base import BaseCommand
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

import logging

class Command(BaseCommand):
    args = "debug"
    help = 'AMQP consumer'
    log = logging.getLogger(__name__)
    
    debug = False
    callback = None
    host = None
    virtual_host = None
    user_name = None
    user_pass = None
    queue = None
    delayed_channel = None
    delay = 20000

    def setup(self, *args, **kwargs):
        if 'debug' in args:
            self.debug = True
        self.host = getattr(settings, 'COM_BROKER', None)
        self.virtual_host = getattr(settings, 'COM_VHOST', None)
        self.user_name = getattr(settings, 'COM_USERNAME', None)
        self.user_pass = getattr(settings, 'COM_PASSWORD', None)
        self.queue = getattr(settings, 'COM_QUEUE', None)
        if not (self.host and self.virtual_host and self.user_name and self.user_pass and self.queue):
            raise ImproperlyConfigured
        callbackfunc_path = getattr(settings, 'CONSUMER_CALLBACK', None)
        if not callbackfunc_path:
            raise ImproperlyConfigured
        else:
            parts = callbackfunc_path.split('.')
            module = __import__('.'.join(parts[:-1]), fromlist=[parts[-1]])
            self.callback = getattr(module, parts[-1])
            if not(callable(self.callback)):
                   raise ImproperlyConfigured
        self.ttl = getattr(settings, 'DJCONSUMER_TTL', 20000)

        self.log.info(u"------ Consumer setup ------")
        self.log.info(u"- Host: " + self.host + u" -")
        self.log.info(u"- VH: " + self.virtual_host + u" -")
        self.log.info(u"- Username: " + self.user_name + u" -")
        self.log.info(u"- Password: " + self.user_pass + u" -")
        self.log.info(u"- Queue: " + self.queue + u" -")
        self.log.info(u"- Callback: " + str(self.callback) + u" -")
            
    def task_do(self,channel, method, header_frame, body):
        result = {}
        if self.debug:
            self.log.info(u"New task" + body)
        try:
            result = self.callback(header_frame, body)
            if type(result) != type({}):
                result = {}
        except Exception as e:
            self.log.error(u"ERROR on callback function: " + str(e))
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return 1
        msg = result.get('msg', '')
        retry = result.get('retry', False)
        if retry:
            if self.debug:
                self.log.info('Retrying task. Reason: %s, task: %s, requeue: %s' % (msg, body, str(retry)))
            if header_frame.expiration > 0:
                self.delayed_channel.basic_publish(exchange='',
                                               routing_key=self.queue + '_delayed',
                                               body=body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return 1
    

    def handle(self, *args, **options):
        self.setup(*args)
        credentials = pika.PlainCredentials(self.user_name, self.user_pass)
        parameters = pika.ConnectionParameters(host=self.host, virtual_host=self.virtual_host,
                                               credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        delayed_connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        channel.queue_bind(exchange='amq.direct',
                           queue=self.queue)
        self.delayed_channel = delayed_connection.channel()
        self.delayed_channel.queue_declare(queue=self.queue + '_delayed', arguments={
            'x-message-ttl' : self.ttl, 
            'x-dead-letter-exchange' : 'amq.direct', 
            'x-dead-letter-routing-key' : self.queue,
            })
        channel.basic_consume(self.queue, self.task_do)
        if self.debug:
            self.log.info(u"Start consuming queue...")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.log.info(u"Stop consuming and close connections")
            channel.stop_consuming()
        connection.close()
        delayed_connection.close()
        
