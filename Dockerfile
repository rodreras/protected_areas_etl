FROM apache/airflow:3.1.0-python3.12

#  System dependencies 
USER root

RUN apt-get update && apt-get install --no-install-recommends -y \
        gcc \
        gdal-bin \
        libgdal-dev \
        libgdal32 \
        libpq-dev \
        libproj-dev && \
    rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    GDAL_CONFIG=/usr/bin/gdal-config

# Python dependencies 
# uv resolves pyproject.toml + uv.lock and installs directly into Airflow's
# Python (no venv) so every worker task can import the packages.
USER airflow

COPY --chown=airflow:airflow pyproject.toml uv.lock /tmp/build/

RUN pip install --no-cache-dir uv==0.9.8 && \
    cd /tmp/build && \
    uv export --locked --no-hashes --no-emit-project -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -rf /tmp/build /tmp/requirements.txt

# ── Project source 
# /opt/airflow/src sits next to /opt/airflow/dags, so sys.path.append("..")
# inside any DAG file resolves to this directory automatically.
COPY --chown=airflow:airflow src /opt/airflow/src
