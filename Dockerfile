FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.0}
COPY . .

RUN pip install --user --upgrade pip
RUN pip install --user --no-cache-dir "apache-airflow==${AIRFLOW_VERSION:-'3.1.0'}" -r requirements_v1.txt