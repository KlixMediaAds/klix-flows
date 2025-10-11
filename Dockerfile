FROM prefecthq/prefect:3-latest
ENV PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY flows/ flows/
COPY prefect.yaml prefect.yaml
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
