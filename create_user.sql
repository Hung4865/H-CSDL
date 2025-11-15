-- 1. Chuyển sang CSDL mới
USE db_streaming_project;
GO

-- 2. "Ánh xạ" Login (ở Server) vào thành User (ở Database)
CREATE USER kafka_user FOR LOGIN kafka_user;
GO

-- 3. Cấp lại các quyền cơ bản (Đọc và Ghi)
ALTER ROLE db_datareader ADD MEMBER kafka_user;
GO
ALTER ROLE db_datawriter ADD MEMBER kafka_user;
GO

-- 4. Cấp quyền EXECUTE (quan trọng nhất)
GRANT EXECUTE ON dbo.sp_ProcessNewOrder TO kafka_user;
GO

PRINT '*** Đã cấp lại toàn bộ quyền cho kafka_user thành công! ***';
