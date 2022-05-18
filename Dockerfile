FROM python:3.8-bullseye


# Remove after direct git reference to Prophet is removed.
RUN apt-get update && apt-get -y install git
RUN mkdir /app

WORKDIR /app

COPY . .
RUN pip install "$(cat dev-requirements.in | grep pip-tools)" && \
    pip install -r dev-requirements.txt

RUN cat requirements.txt | while read requirement || [[ -n $requirement ]]; do pip install -e $requirement; done

ENTRYPOINT [ "soda" ]
CMD [ "scan" ]
