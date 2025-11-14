import json
import pyodbc
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable # <-- THÊM IMPORT NÀY
import time                             # <-- THÊM IMPORT NÀY

KAFKA_TOPIC = 'ecommerce_orders'

# === 1. TẠO KẾT NỐI KAFKA (VỚI VÒNG LẶP THỬ LẠI) ===
consumer = None
connection_retries = 10 # Thử kết nối 10 lần
retry_delay = 5         # Chờ 5 giây giữa mỗi lần thử

print("Bắt đầu kết nối đến Kafka...")
for i in range(connection_retries):
    try:
        # Thử kết nối
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=['kafka-1:29092'], # Tên service đã đúng
            auto_offset_reset='earliest', 
            group_id='sqlserver-consumer-group' 
        )
        # Nếu kết nối thành công, thoát khỏi vòng lặp
        print("Kết nối Kafka thành công!")
        break
        
    except NoBrokersAvailable:
        # Nếu thất bại, báo lỗi và chờ
        print(f"Không thể kết nối đến Kafka. Đang thử lại sau {retry_delay} giây... (lần {i+1}/{connection_retries})")
        time.sleep(retry_delay)
# === KẾT THÚC VÒNG LẶP THỬ LẠI ===


# === 2. KẾT NỐI SQL SERVER (Giữ nguyên code của bạn) ===
def create_sql_server_connection():
    """Hàm tạo kết nối tới CSDL SQL Server trên máy Host"""
    try:
        server_name = 'host.docker.internal' 
        database_name = 'db_streaming_project'
        
        # CÁCH 1: Windows Authentication (Rất có thể sẽ thất bại)
        # conn_str = (
        #     f'DRIVER={{ODBC Driver 17 for SQL Server}};' 
        #     f'SERVER={server_name};'
        #     f'DATABASE={database_name};'
        #     'Trusted_Connection=yes;'
        # )
        
        # CÁCH 2: SQL Server Authentication (NÊN DÙNG CÁCH NÀY)
        # (Hãy đảm bảo SQL Server của bạn cho phép login qua TCP/IP)
        sql_username = 'kafka_user' # <-- THAY USERNAME CỦA BẠN
        sql_password = 'P@ssw0rd123' # <-- THAY PASSWORD CỦA BẠN
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server_name};'
            f'DATABASE={database_name};'
            f'UID={sql_username};'
            f'PWD={sql_password};'
        )

        conn = pyodbc.connect(conn_str)
        print("Kết nối SQL Server thành công!")
        return conn
    except pyodbc.Error as e:
        print(f"Lỗi kết nối SQL Server: {e}")
        return None

def process_message(message):
    """Xử lý tin nhắn và gọi Stored Procedure"""
    try:
        order_data_str = message.value.decode('utf-8')
        print(f"Nhận được đơn hàng: {order_data_str}")

        conn = create_sql_server_connection()
        
        if conn:
            cursor = conn.cursor()
            sql_exec_sp = "EXEC sp_ProcessNewOrder ?" 
            cursor.execute(sql_exec_sp, order_data_str)
            conn.commit()
            print("Đã xử lý và lưu đơn hàng vào SQL Server.")
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Lỗi xử lý tin nhắn: {e}")

def consume_data_stream():
    """Hàm chính: Vòng lặp vô tận để "nghe" Kafka"""
    print("Đang chờ tin nhắn từ Kafka...")
    try:
        for message in consumer:
            process_message(message)
    except KeyboardInterrupt:
        print("Dừng consumer.")
    finally:
        if consumer:
            consumer.close()

# === 3. HÀM MAIN (Đã cập nhật) ===
if __name__ == '__main__':
    if consumer is None:
        print("Không thể kết nối đến Kafka sau nhiều lần thử. Thoát.")
    else:
        # Nếu kết nối thành công, mới bắt đầu "nghe"
        consume_data_stream()