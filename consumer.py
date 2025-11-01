import json
import pyodbc 
from kafka import KafkaConsumer

KAFKA_TOPIC = 'ecommerce_orders'
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', 
    group_id='sqlserver-consumer-group' 
)

# === PHẦN BẮT BUỘC SỬA ===
def create_sql_server_connection():
    try:
        # (1) SỬA TÊN SERVER Ở ĐÂY (Xem trong SSMS lúc login)
        server_name = 'LAPTOP-6IB52RQP' 
        database_name = 'db_streaming_project'
        
        # (2) CHỌN CÁCH ĐĂNG NHẬP
        
        # CÁCH 1: Nếu login SSMS bằng "Windows Authentication" (không cần pass)
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};' # Sửa '17' nếu bạn dùng driver 18
            f'SERVER={server_name};'
            f'DATABASE={database_name};'
            'Trusted_Connection=yes;' 
        )

        # CÁCH 2: Nếu login SSMS bằng "SQL Server Authentication" (dùng user/pass)
        # (Comment 3 dòng của Cách 1 nếu dùng cách này)
        # sql_username = 'sa' 
        # sql_password = 'YOUR_PASSWORD_HERE' 
        # conn_str = (
        #     f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        #     f'SERVER={server_name};'
        #     f'DATABASE={database_name};'
        #     f'UID={sql_username};'
        #     f'PWD={sql_password};'
        # )

        conn = pyodbc.connect(conn_str)
        print("Kết nối SQL Server thành công!")
        return conn
    except pyodbc.Error as e:
        print(f"Lỗi kết nối SQL Server: {e}")
        return None
# === KẾT THÚC PHẦN SỬA ===

def process_message(message):
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
    print("Bắt đầu 'nghe' (listen) dữ liệu từ Kafka...")
    for message in consumer:
        process_message(message)

if __name__ == '_main_':
    consume_data_stream()
