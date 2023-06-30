# 베이스 이미지 선택
FROM python:3.9-slim-buster

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 파일 복사
COPY silverwork_dbt/ /app/silverwork_dbt/

# 의존성 설치
RUN pip install dbt-snowflake pandas
