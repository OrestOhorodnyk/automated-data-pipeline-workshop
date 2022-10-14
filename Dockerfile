FROM apache/airflow:2.3.3-python3.7
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential libopenmpi-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY ./requirements.txt /tmp
RUN pip install --no-cache-dir -r /tmp/requirements.txt

