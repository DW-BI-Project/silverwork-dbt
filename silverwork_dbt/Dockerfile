# 베이스 이미지 선택
FROM --platform=linux/amd64 python:3.9-slim

# 작업 디렉토리 설정
WORKDIR /app

COPY . .

RUN pip install dbt-snowflake dbt-bigquery pandas

RUN dbt deps