# 1. CHỌN IMAGE CƠ SỞ
FROM python:3.10-slim

# 2. TẠO THƯ MỤC LÀM VIỆC
WORKDIR /app

# 3. CÀI ĐẶT CÁC GÓI PHỤ THUỘC CỦA HỆ THỐNG
# === CÀI ĐẶT MICROSOFT ODBC DRIVER 17 (Phương pháp hiện đại) ===
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev

RUN mkdir -p /usr/share/keyrings

RUN curl https://packages.microsoft.com/keys/microsoft.asc | \
    gpg --dearmor -o /usr/share/keyrings/microsoft.gpg

# 4. Thêm repo của Microsoft (ĐÂY LÀ DÒNG ĐÃ SỬA)
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" \
    > /etc/apt/sources.list.d/mssql-release.list

# 5. Cập nhật lại apt (để nhận repo mới) và cài đặt driver
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# 6. Dọn dẹp cache của apt để giảm kích thước image
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# === KẾT THÚC CÀI ĐẶT ODBC ===

# 7. CÀI ĐẶT THƯ VIỆN PYTHON
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 8. SAO CHÉP MÃ NGUỒN
COPY . .

# 9. LỆNH ĐỂ CHẠY ỨNG DỤNG
CMD ["python", "consumer.py"]
