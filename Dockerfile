ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim

COPY . ./

RUN pip install -e .

ENTRYPOINT ["prefect", "worker", "start", "--type", "kubernetes"]
