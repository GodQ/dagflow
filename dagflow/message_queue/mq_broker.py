import pika
import pika.exceptions
import json
import time


class MQ_Broker:
    def __init__(self, url, queue, exchange=None, exchange_type='direct', delay_queue=None):
        self.url = url
        self.queue = queue
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.delay_queue = delay_queue
        self.__connection, self.__channel = self.connect()
        self.init_broker_queue()
        if delay_queue:
            self.init_dl_queue()

    def connect(self):
        url = self.url
        print("Connecting...")
        connection = pika.BlockingConnection(pika.URLParameters(url))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        return connection, channel

    @property
    def channel(self):
        if not self.__connection or self.__connection.is_closed:
            self.__connection, self.__channel = self.connect()
        return self.__channel

    def init_dl_queue(self):
        channel = self.channel
        dl_queue_name = self.delay_queue
        normal_queue_name = self.queue
        exchange_name = self.exchange

        if not channel:
            raise Exception("No Channel and No Url to create dead letter queue")
        if exchange_name is None:
            exchange_name = ""
        dl_queue_args = {
            "x-dead-letter-exchange": exchange_name,
            "x-dead-letter-routing-key": normal_queue_name,
            # "x-message-ttl": 2 * 60 * 60 * 1000
        }
        # channel.queue_delete(dl_queue_name)
        channel.queue_declare(dl_queue_name, durable=True, arguments=dl_queue_args)
        if exchange_name:
            channel.exchange_declare(exchange_name, exchange_type='direct', durable=True)
            channel.queue_bind(dl_queue_name, exchange_name)

    def init_broker_queue(self):
        channel = self.channel
        queue_name = self.queue
        exchange_name = self.exchange

        if not channel:
            raise Exception("No Channel and No Url to create broker queue")
        channel.queue_declare(queue_name, durable=True)
        if exchange_name:
            channel.exchange_declare(exchange_name, exchange_type='direct', durable=True)
            channel.queue_bind(queue_name, exchange_name)

    @staticmethod
    def demo_on_message(channel, method_frame, header_frame, body):
        print("now: ", time.time())
        print('method_frame.delivery_tag', method_frame.delivery_tag)
        print('body', body)
        print()
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def run_listener(self, on_message):
        channel = self.channel
        queue_name = self.queue
        while True:
            try:
                channel.basic_consume(queue_name, on_message)
                try:
                    print("consuming...")
                    channel.start_consuming()
                except KeyboardInterrupt:
                    channel.stop_consuming()
                    channel.connection.close()
                    break
            # The AMQP connection was closed
            except pika.exceptions.ConnectionClosed:
                continue
            # Do not recover on channel errors
            except pika.exceptions.AMQPChannelError as err:
                print("Caught a channel error: {}, stopping...".format(err))
                break
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                print("Connection was closed, retrying...")
                continue

    def __publish_message(self, queue_name, body, expiration=None):
        channel = self.channel
        exchange_name = self.exchange

        if isinstance(body, dict):
            body = json.dumps(body)

        properties = None
        if expiration:
            properties = pika.BasicProperties(expiration=str(expiration))

        # Send a message
        r = channel.basic_publish(
            exchange=exchange_name,
            routing_key=queue_name,
            body=body,
            properties=properties
        )
        print("now: ", time.time())
        print(r)
        return True

    def send_msg(self, body):
        queue_name = self.queue
        return self.__publish_message(queue_name=queue_name, body=body)

    def send_delayed_msg(self, body, delay_seconds):
        queue_name = self.delay_queue
        return self.__publish_message(queue_name=queue_name, body=body, expiration=str(delay_seconds * 1000))
