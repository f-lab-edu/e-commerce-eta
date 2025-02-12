import json
import pika
import boto3
from abc import ABC, abstractmethod

# 메시지 전송 인터페이스 (추상 클래스)
class MessageSender(ABC):
    @abstractmethod
    def send(self, event_data):
        pass

# RabbitMQ 전송 클래스
class RabbitMQSender(MessageSender):
    def __init__(self, host='localhost', queue_name='delivery_events', user='dawn', password='dawn'):
        self.host = host
        self.queue_name = queue_name
        self.credentials = pika.PlainCredentials(user, password)

    def send(self, event_data):
        """RabbitMQ로 메시지 전송"""
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host, credentials=self.credentials)
            )
            channel = connection.channel()

            channel.queue_declare(queue=self.queue_name, durable=True)

            # 메시지 전송
            channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(event_data),
            )

            print(f"Sent event to RabbitMQ: {event_data['event_id']}")
            connection.close()
        except Exception as e:
            print(f"RabbitMQ Send Failed: {str(e)}")

# AWS SQS 전송 클래스 (향후 확장 가능)
class SQSSender(MessageSender):
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs = boto3.client('sqs')  # AWS SQS 클라이언트

    def send(self, event_data):
        """AWS SQS로 메시지 전송"""
        try:
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(event_data)
            )
            print(f"Sent event to SQS: {event_data['event_id']} (MessageId: {response['MessageId']})")
        except Exception as e:
            print(f"SQS Send Failed: {str(e)}")

# 팩토리 패턴 적용: MessageSenderFactory
class MessageSenderFactory:
    @staticmethod
    def get_sender(sender_type: str, **kwargs) -> MessageSender:
        """팩토리 메서드: 메시지 전송 객체 생성"""
        if sender_type == "rabbitmq":
            return RabbitMQSender(
                host=kwargs.get("host", "localhost"),
                queue_name=kwargs.get("queue_name", "delivery_events"),
                user=kwargs.get("user", "dawn"),
                password=kwargs.get("password", "dawn"),
            )
        elif sender_type == "sqs":
            return SQSSender(queue_url=kwargs["queue_url"])
        else:
            raise ValueError(f"지원하지 않는 메시지 전송 방식: {sender_type}")

