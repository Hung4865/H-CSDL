import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
KAFKA_TOPIC = 'ecommerce_orders'

def create_random_order():
    return {
        "product_id": random.randint(1, 5), # Phải khớp với 5 sản phẩm bạn tạo ở SQL
        "quantity": random.randint(1, 3)
    }

def send_data_stream():
    print("Bắt đầu đẩy dữ liệu lên Kafka...")
    while True:
        try:
            order_data = create_random_order()
            print(f"Gửi đơn hàng: {order_data}")
            producer.send(KAFKA_TOPIC, order_data)
            producer.flush() 
            time.sleep(2) 
        except Exception as e:
            print(f"Lỗi: {e}")
            time.sleep(5) 

if __name__ == '_main_':
    send_data_stream()