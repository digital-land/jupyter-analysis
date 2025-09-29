FROM apache/airflow:2.8.0

USER root

# Install Python and system dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev libffi-dev libssl-dev curl

# Copy your monitoring tool
COPY monitoring_data_collection_tool /opt/airflow/monitoring_data_collection_tool

# Install Python requirements
COPY monitoring_data_collection_tool/documentation/utils/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER airflow
