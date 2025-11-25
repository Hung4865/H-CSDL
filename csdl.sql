
-- Chuyển về CSDL master để có thể xóa CSDL dự án
USE master;
GO

-- Xóa CSDL cũ nếu nó tồn tại
IF DB_ID('db_streaming_project') IS NOT NULL
BEGIN
    -- Đóng mọi kết nối đang dùng CSDL này
    ALTER DATABASE db_streaming_project SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    -- Xóa CSDL
    DROP DATABASE db_streaming_project;
    PRINT 'Đã xóa CSDL db_streaming_project cũ.';
END
GO

-- 1. TẠO DATABASE MỚI
PRINT 'Đang tạo Database db_streaming_project...';
CREATE DATABASE db_streaming_project;
GO

-- Chuyển sang CSDL mới
USE db_streaming_project;
GO

/* ========== 2. TẠO CÁC BẢNG (6 BẢNG CHÍNH + LOG) ========== */
-- Các bảng "Chiều" (Dimensions) không phụ thuộc
PRINT 'Đang tạo bảng categories...';
CREATE TABLE categories (
    category_id INT IDENTITY(1,1) PRIMARY KEY,
    category_name NVARCHAR(100) NOT NULL UNIQUE
);
GO

PRINT 'Đang tạo bảng users (Tự động điền)...';
CREATE TABLE users (
    user_id INT IDENTITY(1,1) PRIMARY KEY,
    gender NVARCHAR(10) NOT NULL,
    age INT NOT NULL,
    occupation NVARCHAR(50) NOT NULL,
    CONSTRAINT UQ_User_Profile UNIQUE (gender, age, occupation)
);
GO

PRINT 'Đang tạo bảng shippers...';
CREATE TABLE shippers (
    shipper_id INT IDENTITY(1,1) PRIMARY KEY,
    shipper_name NVARCHAR(100) NOT NULL UNIQUE
);
GO

-- Bảng phụ thuộc (Phải tạo sau)
PRINT 'Đang tạo bảng products...';
CREATE TABLE products (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0,
    category_id INT,
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);
GO

PRINT 'Đang tạo bảng orders...';
CREATE TABLE orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    created_at DATETIME DEFAULT GETDATE(),
    total_amount DECIMAL(10, 2) NOT NULL,
    user_id INT,
    shipper_id INT,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (shipper_id) REFERENCES shippers(shipper_id)
);
GO

PRINT 'Đang tạo bảng order_details...';
CREATE TABLE order_details (
    order_detail_id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price_at_purchase DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
GO

PRINT 'Đang tạo bảng inventory_logs (Cho Trigger)...';
CREATE TABLE inventory_logs (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    change_amount INT NOT NULL,
    reason NVARCHAR(255),
    log_time DATETIME DEFAULT GETDATE()
);
GO

/* ========== 3. THÊM DỮ LIỆU MẪU ========== */
PRINT 'Đang thêm categories mẫu...';
INSERT INTO categories (category_name) VALUES
(N'Laptop'), (N'Phụ kiện'), (N'Màn hình');
GO

PRINT 'Đang thêm shippers mẫu...';
INSERT INTO shippers (shipper_name) VALUES
(N'Giao Hàng Nhanh'), (N'Viettel Post'), (N'Grab Express');
GO

PRINT 'Đang thêm 27 sản phẩm mẫu...';
-- (Thêm 27 sản phẩm và set tồn kho 1000)
INSERT INTO products (name, price, stock_quantity, category_id) VALUES
(N'Laptop Gaming A', 2500.00, 1000, 1),
(N'Chuột Bluetooth B', 75.00, 1000, 2),
(N'Bàn phím cơ C', 120.00, 1000, 2),
(N'Màn hình 4K D', 450.00, 1000, 3),
(N'Tai nghe E', 180.00, 1000, 2),
(N'Laptop Ultra Pro', 3500.00, 1000, 1),
(N'Macbook Air M3', 4200.00, 1000, 1),
(N'Bàn phím cơ RGB', 180.00, 1000, 2),
(N'Chuột không dây Silent', 55.00, 1000, 2),
(N'Màn hình cong 32-inch', 750.00, 1000, 3),
(N'Ổ cứng SSD 2TB', 300.00, 1000, 2),
(N'Webcam 4K Pro', 220.00, 1000, 2),
(N'Màn hình di động 16-inch', 350.00, 1000, 3),
(N'Laptop Văn phòng Zen', 1800.00, 1000, 1),
(N'Laptop Đồ họa ProMax', 5500.00, 1000, 1),
(N'Ultrabook Mỏng nhẹ X1', 2100.00, 1000, 1),
(N'Bàn phím không dây Slim', 85.00, 1000, 2),
(N'Chuột công thái học Master', 120.00, 1000, 2),
(N'Tai nghe True Wireless Z', 150.00, 1000, 2),
(N'Loa Bluetooth BassBoost', 110.00, 1000, 2),
(N'Balo laptop chống sốc', 70.00, 1000, 2),
(N'Đế tản nhiệt laptop RGB', 65.00, 1000, 2),
(N'Bộ chia USB-C 8-in-1', 90.00, 1000, 2),
(N'Pin sạc dự phòng 20000mAh', 50.00, 1000, 2),
(N'Miếng lót chuột (Size L)', 25.00, 1000, 2),
(N'Giá đỡ laptop nhôm', 45.00, 1000, 2),
(N'Màn hình 2K 27-inch 144Hz', 550.00, 1000, 3);
GO

/* ========== 4. TẠO STORED PROCEDURE ========== */
PRINT 'Đang tạo Stored Procedure sp_ProcessNewOrder...';
GO
CREATE PROCEDURE sp_ProcessNewOrder
    @p_OrderData NVARCHAR(MAX)
AS
BEGIN
    SET XACT_ABORT ON;
    SET NOCOUNT ON; 

    DECLARE @v_product_id INT, @v_quantity INT, @v_gender NVARCHAR(10);
    DECLARE @v_age INT, @v_occupation NVARCHAR(50), @v_user_id INT;
    DECLARE @v_shipper_id INT;
    DECLARE @v_current_stock INT, @v_product_price DECIMAL(10, 2);
    DECLARE @v_total_amount DECIMAL(10, 2), @v_new_order_id INT;

    BEGIN TRY
        BEGIN TRANSACTION;

        SET @v_product_id = JSON_VALUE(@p_OrderData, '$.product_id');
        SET @v_quantity = JSON_VALUE(@p_OrderData, '$.quantity');
        SET @v_gender = JSON_VALUE(@p_OrderData, '$.gender');
        SET @v_age = JSON_VALUE(@p_OrderData, '$.age');
        SET @v_occupation = JSON_VALUE(@p_OrderData, '$.occupation');
        SET @v_shipper_id = JSON_VALUE(@p_OrderData, '$.shipper_id');

        SELECT @v_user_id = user_id
        FROM users
        WHERE gender = @v_gender AND age = @v_age AND occupation = @v_occupation;
        
        IF @v_user_id IS NULL
        BEGIN
            INSERT INTO users (gender, age, occupation)
            VALUES (@v_gender, @v_age, @v_occupation);
            SET @v_user_id = SCOPE_IDENTITY();
        END
        
        SELECT 
            @v_product_price = price, 
            @v_current_stock = stock_quantity
        FROM products WITH (UPDLOCK)
        WHERE product_id = @v_product_id;

        IF @v_current_stock >= @v_quantity
        BEGIN
            SET @v_total_amount = @v_product_price * @v_quantity;

            INSERT INTO orders (total_amount, user_id, shipper_id)
            VALUES (@v_total_amount, @v_user_id, @v_shipper_id);
            
            SET @v_new_order_id = SCOPE_IDENTITY(); 

            INSERT INTO order_details (order_id, product_id, quantity, price_at_purchase)
            VALUES (@v_new_order_id, @v_product_id, @v_quantity, @v_product_price);

            UPDATE products
            SET stock_quantity = stock_quantity - @v_quantity
            WHERE product_id = @v_product_id;

            COMMIT TRANSACTION;
        END
        ELSE
        BEGIN
            RAISERROR('Không đủ hàng tồn kho (product_id: %d).', 16, 1, @v_product_id);
        END
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        PRINT 'Đã xảy ra lỗi: ' + @ErrorMessage;
    END CATCH
END
GO

/* ========== 5. TẠO TRIGGER ========== */
CREATE TRIGGER trg_KiemTraTonKhoKhongAm
ON products 
AFTER UPDATE 
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM inserted i
        WHERE i.stock_quantity < 0
    )
    BEGIN
        
        ROLLBACK TRANSACTION;
        
        RAISERROR (N'Lỗi: Tồn kho sản phẩm không được phép âm. Thao tác UPDATE đã bị hủy.', 16, 1);
        RETURN;
    END
END
GO


/* ========== 6. TẠO VIEW CHO POWER BI ========== */
PRINT 'Đang tạo View v_SalesDashboard...';
GO
CREATE VIEW v_SalesDashboard AS
SELECT
    p.name AS product_name,
    c.category_name,
    od.quantity AS quantity_sold,
    od.price_at_purchase,
    o.total_amount,
    o.created_at AS sale_time,
    sh.shipper_name,
    u.gender,
    u.age,
    u.occupation
FROM orders o
JOIN order_details od ON o.order_id = od.order_id
JOIN products p ON od.product_id = p.product_id
JOIN users u ON o.user_id = u.user_id
JOIN shippers sh ON o.shipper_id = sh.shipper_id
JOIN categories c ON p.category_id = c.category_id;
GO

PRINT '*** TOÀN BỘ SCRIPT ĐÃ TẠO MỚI THÀNH CÔNG! ***';
