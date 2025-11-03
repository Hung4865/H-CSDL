# --- 1. Nhập các thư viện cần thiết ---
import json  # Dùng để giải mã JSON
import pyodbc  # Thư viện chính để "nói chuyện" với SQL Server
from kafka import KafkaConsumer  # Thư viện chính để "nghe" Kafka

# --- 2. Định nghĩa tên Topic (phải khớp) ---
KAFKA_TOPIC = 'ecommerce_orders'

# --- 3. Kết nối tới Kafka Consumer (Người nghe) ---
consumer = KafkaConsumer(
    KAFKA_TOPIC, # Chỉ định Topic muốn "nghe"
    
    # Chỉ định địa chỉ của Kafka Server
    bootstrap_servers=['localhost:9092'],
    
    # Đọc từ tin nhắn cũ nhất (nếu consumer bị tắt và khởi động lại)
    auto_offset_reset='earliest', 
    
    # Đặt tên cho nhóm consumer này
    group_id='sqlserver-consumer-group' 
)

# === 4. PHẦN BẮT BUỘC SỬA (Kết nối SQL Server) ===
def create_sql_server_connection():
    """Hàm tạo kết nối tới CSDL SQL Server"""
    try:
        # (1) SỬA TÊN SERVER Ở ĐÂY
        # Xem tên server của bạn trong SSMS lúc đăng nhập
        # Ví dụ: 'localhost' hoặc 'TEN-MAY-TINH\SQLEXPRESS'
        server_name = 'localhost' 
        
        # Tên CSDL bạn đã tạo
        database_name = 'db_streaming_project'
        
        # (2) CHỌN CÁCH ĐĂNG NHẬP (Windows hay SQL Server)
        
        # CÁCH 1: Nếu login SSMS bằng "Windows Authentication" (không cần pass)
        # Chuỗi kết nối (connection string)
        conn_str = (
            # Chỉ định driver ODBC bạn đã cài (thường là 17 hoặc 18)
            f'DRIVER={{ODBC Driver 17 for SQL Server}};' 
            f'SERVER={server_name};'
            f'DATABASE={database_name};'
            'Trusted_Connection=yes;' # Báo cho SQL Server dùng user Windows
        )

        # CÁCH 2: Nếu login SSMS bằng "SQL Server Authentication" (dùng user/pass)
        # (Comment 3 dòng của Cách 1 nếu dùng cách này)
        # sql_username = 'sa' # ĐIỀN USERNAME CỦA BẠN
        # sql_password = 'YOUR_PASSWORD_HERE' # ĐIỀN MẬT KHẨU CỦA BẠN
        # conn_str = (
        #     f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        #     f'SERVER={server_name};'
        #     f'DATABASE={database_name};'
        #     f'UID={sql_username};'
        #     f'PWD={sql_password};'
        # )

        # Thực hiện kết nối
        conn = pyodbc.connect(conn_str)
        print("Kết nối SQL Server thành công!")
        return conn
    except pyodbc.Error as e:
        # Báo lỗi nếu không kết nối được (sai tên server, sai pass...)
        print(f"Lỗi kết nối SQL Server: {e}")
        return None
# === KẾT THÚC PHẦN SỬA ===

def process_message(message):
    """
    Hàm này được gọi mỗi khi có 1 tin nhắn mới từ Kafka.
    Nhiệm vụ: Lấy JSON và ném vào Stored Procedure.
    """
    try:
        # 1. Giải mã tin nhắn. 'message.value' là data,
        # .decode('utf-8') để biến nó từ bytes thành chuỗi JSON
        order_data_str = message.value.decode('utf-8')
        print(f"Nhận được đơn hàng: {order_data_str}")

        # 2. Tạo kết nối mới tới CSDL
        conn = create_sql_server_connection()
        
        # 3. Chỉ thực thi nếu kết nối thành công
        if conn:
            cursor = conn.cursor() # Tạo một "con trỏ"
            
            # 4. Định nghĩa lệnh gọi Stored Procedure
            # 'EXEC' là lệnh của T-SQL, '?' là tham số (placeholder)
            sql_exec_sp = "EXEC sp_ProcessNewOrder ?" 
            
            # 5. Thực thi SP và truyền chuỗi JSON (order_data_str) vào
            cursor.execute(sql_exec_sp, order_data_str)
            
            # 6. (Rất quan trọng) Xác nhận (commit) giao dịch
            # Nếu không có lệnh này, SP chạy nhưng data không được lưu
            conn.commit()
            
            print("Đã xử lý và lưu đơn hàng vào SQL Server.")
            
            # 7. Đóng kết nối
            cursor.close()
            conn.close()
            
    except Exception as e:
        # Bắt lỗi nếu SP bị ROLLBACK (ví dụ: hết hàng, hoặc lỗi 266 cũ)
        print(f"Lỗi xử lý tin nhắn: {e}")

def consume_data_stream():
    """Hàm chính: Vòng lặp vô tận để "nghe" Kafka"""
    
    print("Bắt đầu 'nghe' (listen) dữ liệu từ Kafka...")
    
    # Vòng lặp này sẽ "treo" (ngủ) ở đây
    # Nó chỉ "thức dậy" và chạy code bên trong
    # khi có 1 tin nhắn mới trong topic
    for message in consumer:
        # Khi có tin nhắn, gọi hàm xử lý
        process_message(message)

# --- 6. Bắt đầu chạy chương trình ---
# (Đã sửa lỗi _name_ thành __name__)
if __name__ == '__main__':
    consume_data_stream()
