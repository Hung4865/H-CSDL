import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable  # <-- THÊM IMPORT NÀY
import time                                 # <-- THÊM IMPORT NÀY
from dagster import asset, Definitions, ScheduleDefinition, define_asset_job 

# --- 1. Logic tạo đơn hàng (Giữ nguyên) ---
def create_random_order():
    return {
        "product_id": random.randint(1, 27), 
        "quantity": random.randint(1, 3), 
        "gender": random.choice(['Nam', 'Nữ']),
        "age": random.randint(18, 60),
        "occupation": random.choice(['Đi học', 'Đi làm']),
        "shipper_id": random.randint(1, 3) 
    }

# --- 2. Định nghĩa Asset (ĐÃ THÊM VÒNG LẶP THỬ LẠI) ---
@asset
def kafka_orders(context): # Thêm context để log tốt hơn
    """Một tài sản tạo 50 đơn hàng giả và đẩy vào Kafka."""
    
    producer = None
    connection_retries = 10 # Thử 10 lần
    retry_delay = 5         # Mỗi lần cách 5 giây

    context.log.info("Producer: Bắt đầu kết nối đến Kafka...")
    for i in range(connection_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka-1:29092'], 
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            context.log.info("Producer: Kết nối Kafka thành công!")
            break # Thoát vòng lặp nếu thành công
        except NoBrokersAvailable:
            context.log.warning(f"Producer: Không thể kết nối. Đang thử lại sau {retry_delay} giây... (lần {i+1}/{connection_retries})")
            time.sleep(retry_delay)

    if producer is None:
        # Vẫn không kết nối được, văng lỗi để Dagster báo "Failed"
        raise Exception("Producer: Không thể kết nối Kafka sau nhiều lần thử.")

    # Nếu kết nối thành công, tiếp tục gửi
    context.log.info("Đang đẩy 50 đơn hàng...")
    for _ in range(50):
        order_data = create_random_order()
        producer.send('ecommerce_orders', order_data)
        
    producer.flush()
    context.log.info("Đã đẩy 50 đơn hàng thành công.")

# --- 3. TẠO JOB (Giữ nguyên) ---
kafka_orders_job = define_asset_job(
    name="kafka_orders_job",
    selection=[kafka_orders] 
)

# --- 4. Lịch chạy (Giữ nguyên) ---
every_minute_schedule = ScheduleDefinition(
    job=kafka_orders_job, 
    cron_schedule="* * * * *",
)

# --- 5. Đóng gói (Giữ nguyên) ---
defs = Definitions(
    assets=[kafka_orders],
    jobs=[kafka_orders_job], 
    schedules=[every_minute_schedule]
)