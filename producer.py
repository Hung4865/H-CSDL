# --- 1. Nhập các thư viện cần thiết ---
import time  # Dùng để "ngủ" (delay)
import json  # Dùng để mã hóa/giải mã data sang JSON
import random  # Dùng để tạo data ngẫu nhiên
from kafka import KafkaProducer  # Thư viện chính để "nói chuyện" với Kafka
from faker import Faker  # Thư viện tạo data giả (không bắt buộc, chỉ để cho vui)

fake = Faker()

# --- 2. Kết nối tới Kafka Server (Broker) ---
# Tạo một "Producer" (Người gửi)
producer = KafkaProducer(
    # Chỉ định địa chỉ của Kafka Server. 'localhost:9092' là địa chỉ mặc định.
    bootstrap_servers=['localhost:9092'],
    
    # Chỉ định cách mã hóa data trước khi gửi.
    # Ở đây, chúng ta bảo nó hãy biến data (dictionary) thành 1 chuỗi JSON.
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- 3. Định nghĩa tên Topic (Hàng đợi) ---
# Tên này phải KHỚP 100% với tên bạn tạo bằng lệnh Terminal
KAFKA_TOPIC = 'ecommerce_orders'

def create_random_order():
    """
    Hàm này "bịa" ra một đơn hàng ngẫu nhiên.
    Nó trả về một 'dictionary' (giống đối tượng JSON).
    """
    
    # --- Tạo data nhân khẩu học ngẫu nhiên ---
    genders = ['Nam', 'Nữ']
    occupations = ['Đi học', 'Đi làm']
    
    random_gender = random.choice(genders)
    random_occupation = random.choice(occupations)
    
    # Tạo tuổi hợp lý một chút
    if random_occupation == 'Đi học':
        random_age = random.randint(18, 24)
    else:
        random_age = random.randint(25, 60)
    # --- Hết phần data nhân khẩu học ---

    # 4. Trả về dictionary (sẽ được chuyển thành JSON)
    return {
        # Chọn ngẫu nhiên 1 sản phẩm từ 1 đến 27
        "product_id": random.randint(1, 27), 
        
        # Mua ngẫu nhiên 1-3 cái
        "quantity": random.randint(1, 3),   
        
        # Data cho bảng Users (Để SP thực hiện logic "Find-or-Create")
        "gender": random_gender,
        "age": random_age,
        "occupation": random_occupation,
        
        # Data cho bảng Shippers (Chọn ngẫu nhiên 1 trong 3 shipper mẫu)
        "shipper_id": random.randint(1, 3) 
    }

def send_data_stream():
    """
    Đây là hàm chính, chạy vòng lặp vô tận để gửi data.
    """
    print("Bắt đầu đẩy dữ liệu lên Kafka...")
    
    # Vòng lặp này sẽ chạy mãi mãi
    while True:
        try:
            # 1. Tạo 1 đơn hàng ngẫu nhiên
            order_data = create_random_order()
            print(f"Gửi đơn hàng: {order_data}")
            
            # 2. Gửi đơn hàng (dictionary) vào Topic
            producer.send(KAFKA_TOPIC, order_data)
            
            # 3. (Rất quan trọng) Đẩy data đi ngay lập tức
            producer.flush() 
            
            # 4. Nghỉ 4 giây rồi mới lặp lại
            time.sleep(4) 
            
        except Exception as e:
            # Bắt lỗi nếu Kafka Server bị sập
            print(f"Lỗi Producer: {e}")
            time.sleep(5) # Đợi 5s rồi thử lại

# --- 5. Bắt đầu chạy chương trình ---
# Dòng này đảm bảo hàm send_data_stream() chỉ chạy khi bạn
# chạy file này trực tiếp (python producer.py)
if __name__ == '__main__':
    send_data_stream()
