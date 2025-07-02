FROM apache/airflow:2.8.0-python3.9

USER root
COPY monitoring_data_collection_tool /opt/airflow/monitoring_data_collection_tool
COPY monitoring_data_collection_tool/documentation/utils/requirements.txt /tmp/requirements.txt

# Install OS-level dependencies first (still as root)
RUN apt-get update && \
    apt-get install -y gcc python3-dev libffi-dev libssl-dev curl && \
    apt-get clean

# Switch to airflow user before installing Python requirements
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt
