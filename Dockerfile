FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.1.0}
COPY . .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements_v1.txt
#RUN pip install --user --upgrade pip
#RUN pip install --no-cache-dir --user -r /requirements_v1.txt