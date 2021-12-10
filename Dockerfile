FROM python:3.7

RUN apt-get update && apt-get -y install gcc libsasl2-dev python-dev unixodbc-dev

RUN mkdir /app

WORKDIR /app

RUN pip install --upgrade pip

COPY . .

RUN pip install "$(cat dev-requirements.in | grep pip-tools)" && \
    pip install -r dev-requirements.txt && \
    pip install -r requirements.txt

ENTRYPOINT [ "soda" ]
CMD [ "scan" ]
