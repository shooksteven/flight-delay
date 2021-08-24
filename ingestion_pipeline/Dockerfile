# This docker image runs the ingestion code every minute

FROM python:3.7-alpine

# Install java 8 for pyspark
RUN apk add openjdk8 bash
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk

WORKDIR /opt/application/

RUN mkdir data data/archive data/parquet

COPY src/ src/
RUN pip3 install -r src/requirements.txt
RUN touch /var/log/flight_pipeline.log

ADD cronschedule.crontab /tmp/
RUN crontab /tmp/cronschedule.crontab

CMD [ "crond", "-l", "2", "-f" ]