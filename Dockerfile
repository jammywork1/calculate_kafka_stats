FROM docker-proxy.tcsbank.ru/python:3.6-buster

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY . /app/

ENTRYPOINT [ "python3", "/app/src/main.py" ]
