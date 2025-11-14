# Dùng image Python 3.10
FROM python:3.10-slim

# Cài đặt các thư viện Python
COPY requirements.txt .
RUN pip install -r requirements.txt

# Tạo thư mục làm việc và copy code
WORKDIR /app
COPY ./my_pipeline /app/my_pipeline

# Chạy Dagit UI trên cổng 3000 khi container khởi động
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "my_pipeline.assets"]