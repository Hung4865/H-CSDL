![Sơ đồ pipeline](my_streaming_project/Diagram.png)

🛠️ Công Nghệ Sử Dụng

Orchestration: Docker & Docker Compose
Producer: Python, Dagster
Broker (Hàng đợi): Kafka (và Zookeeper)
Consumer: Python, `kafka-python`, `pyodbc`
Database: Microsoft SQL Server (chạy trên máy Host)

---

 🛑 Bước 1: Chuẩn Bị Môi Trường (Rất quan trọng)
1. Cài Đặt Docker
Đảm bảo bạn đã cài đặt Docker Desktop và Docker đang ở trạng thái "Running".

3. Cài Đặt SQL Server (trên máy Host)
Pipeline này được thiết kế để kết nối với SQL Server đang chạy *bên ngoài* Docker (trên máy Windows của bạn).

1.  **Chạy Script SQL:**
    * Mở SQL Server Management Studio (SSMS).
    
2.  **Kích hoạt "Mixed Mode" và Tạo User:**
    * Trong SSMS, chuột phải vào Server (node gốc) -> **Properties** -> **Security**.
    * Chọn **"SQL Server and Windows Authentication mode"** (Mixed Mode).
    * Đi đến **Security** (thư mục) -> **Logins** -> Chuột phải -> **New Login...**
    * Đặt tên Login: `kafka_user`
    * Chọn **SQL Server authentication** và đặt mật khẩu (ví dụ: `P@ssw0rd123`).
    * **Bỏ tích** ô "Enforce password policy".
    * Bấm **OK**.

3.  **Cấp Quyền Cho User:**
    * Chuột phải vào user `kafka_user` vừa tạo -> **Properties**.
    * Vào tab **User Mapping**.
    * Tích vào database `db_streaming_project`.
    * Ở ô bên dưới, tích vào 3 quyền: `db_datareader`, `db_datawriter`, và `db_owner` (hoặc `GRANT EXECUTE` thủ công).
    * Chạy lệnh này trong SSMS (để cấp quyền chạy SP):
        ```sql
        USE db_streaming_project;
        GO
        GRANT EXECUTE ON dbo.sp_ProcessNewOrder TO kafka_user;
        GO
        ```

4.  **Bật Kết Nối TCP/IP (Cho SQL Server):**
    * Mở **SQL Server Configuration Manager** (tìm trong menu Start).
    * Đi đến `SQL Server Network Configuration` -> `Protocols for MSSQLSERVER`.
    * Chuột phải vào **TCP/IP** -> **Enable**.
    * **Double-click** vào `TCP/IP` -> tab **IP Addresses**.
    * Kéo xuống dưới cùng, phần **IPAll**:
        * Xóa trắng ô `TCP Dynamic Ports`.
        * Gõ **`1433`** vào ô `TCP Port`.
    * Bấm **OK**.
    * Quay lại `SQL Server Services`, chuột phải vào `SQL Server (MSSQLSERVER)` -> **Restart**.

5.  **Mở Cổng Tường Lửa (Firewall):**
    * Mở **Windows Defender Firewall with Advanced Security**.
    * Bấm vào **Inbound Rules** -> **New Rule...**
    * Chọn **Port** -> Next.
    * Chọn **TCP** và gõ **`1433`** vào "Specific local ports" -> Next.
    * Chọn **"Allow the connection"** -> Next.
    * Tích cả 3 ô (Domain, Private, Public) -> Next.
    * Đặt tên (ví dụ: `SQL Server (Docker)`) -> Finish.

---

⚙️ Bước 2: Cài Đặt Dự Án

1.  **Clone Repository:**
    ```bash
    git clone <link-github-cua-ban>
    cd my_streaming_project
    ```

2.  **Cập Nhật Mật Khẩu SQL:**
    * Mở file `consumer_service/consumer.py`.
    * Tìm đến hàm `create_sql_server_connection`.
    * Thay đổi `sql_password = 'YOUR_PASSWORD_HERE'` thành mật khẩu bạn đã tạo ở Bước 1.

    ```python
    # consumer_service/consumer.py
    
    # ...
    sql_username = 'kafka_user' 
    sql_password = 'P@ssw0rd123' # <-- THAY MẬT KHẨU CỦA BẠN VÀO ĐÂY
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server_name};'
    # ...
    ```

3.  **Kiểm Tra Cấu Hình (Quan trọng):**
    * Đảm bảo `consumer_service/consumer.py` kết nối Kafka qua `bootstrap_servers=['kafka-1:29092']`.
    * Đảm bảo `dagster_producer/my_pipeline/assets.py` kết nối Kafka qua `bootstrap_servers=['kafka-1:29092']`.
---

▶️ Bước 3: Chạy Dự Án

1.  **Khởi động Docker Compose:**
    Mở terminal trong thư mục gốc của dự án và chạy:

    ```bash
    (Chỉ lần đầu) Dọn dẹp volume cũ nếu có lỗi "InconsistentClusterId"
    docker-compose down -v
    
    # Build và khởi động tất cả service
    docker-compose up -d --build
    ```

2.  **Chờ 30-60 giây** để Zookeeper, Kafka, và Dagster khởi động hoàn toàn.

---

🚀 Bước 4: Kiểm Tra Pipeline (End-to-End)

1.  **Theo dõi Consumer (Người nhận):**
    Mở một terminal MỚI và chạy:
    ```bash
    docker-compose logs -f consumer_service
    ```
    Bạn sẽ thấy log `Kết nối Kafka thành công!` và `Đang chờ tin nhắn...`. Nó sẽ đứng im (im lặng) là đúng.

2.  **Kích hoạt Producer (Người gửi):**
    * Mở trình duyệt và truy cập: **`http://localhost:3000`** (Giao diện Dagster).
    * Đi đến tab **Catalog**.
    * Tích vào ô vuông bên cạnh `kafka_orders`.
    * Bấm nút **Materialize selected** ở góc trên bên phải.

3.  **Xem Kết Quả:**
    * Nhìn ngay vào terminal (Bước 1), bạn sẽ thấy log của **Consumer** chạy ầm ầm:
        ```log
        Nhận được đơn hàng: {"product_id": 10, ...}
        Kết nối SQL Server thành công!
        Đã xử lý và lưu đơn hàng vào SQL Server.
        ```

4.  **Kiểm tra Database (Đích đến):**
    * Mở **SSMS** và chạy truy vấn này để xem dữ liệu mới nhất:
    ```sql
    USE db_streaming_project;
    GO
    
    SELECT TOP 50 * FROM v_SalesDashboard
    ORDER BY sale_time DESC;
    ```
