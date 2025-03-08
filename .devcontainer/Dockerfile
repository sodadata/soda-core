FROM mcr.microsoft.com/devcontainers/python:1-3.9-bookworm

ARG SPARK_VERSION=3.5.5
ARG HADOOP_VERSION=3.4.1
ARG OPENJDK_VERSION=17

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    alien \
    build-essential \
    curl \
    git \
    procps \
    libffi-dev \
    libpq-dev \
    libssl-dev \
    lsb-release \
    odbcinst \
    openjdk-${OPENJDK_VERSION}-jre-headless \
    unixodbc-dev

# Install Microsoft ODBC driver 18 for SQL Server https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    echo "deb [arch=$(dpkg --print-architecture)] https://packages.microsoft.com/debian/12/prod $(lsb_release -cs) main" > /etc/apt/sources.list.d/microsoft-prod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y libsasl2-dev odbcinst mssql-tools18 msodbcsql18 unixodbc-dev

# Dremio support
RUN curl -L https://download.dremio.com/arrow-flight-sql-odbc-driver/arrow-flight-sql-odbc-driver-LATEST.x86_64.rpm -o arrow-driver.rpm && \
    alien -iv --scripts arrow-driver.rpm

# Spark support
ENV SPARK_HOME=/usr/local/spark

RUN HADOOP_MAJOR_VERSION=`echo ${HADOOP_VERSION} | cut -d. -f1` && \
    wget -qO "spark.tgz" "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz"; \
    tar xzf "spark.tgz" -C /usr/local --owner root --group root --no-same-owner && \
    rm "spark.tgz" && \
    ln -s "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}" "${SPARK_HOME}"

# Install native Hadoop libraries
RUN wget -qO "hadoop.tgz" "https://www.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"; \
    tar xzf "hadoop.tgz" -C /opt/ --owner root --group root --no-same-owner && \
    rm "hadoop.tgz" && \
    ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop && \
    mkdir /opt/hadoop-$HADOOP_VERSION/logs

# Configure Spark and Hadoop
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"
ENV HADOOP_HOME="/opt/hadoop-${HADOOP_VERSION}"
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV HADOOP_COMMON_LIB_NATIVE_DIR="${HADOOP_HOME}/lib/native"
ENV HADOOP_OPTS="${HADOOP_OPTS} -Djava.library.path=${HADOOP_HOME}/lib/native"
ENV LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native"

ENV PATH="${PATH}:${SPARK_HOME}/bin:${HADOOP_HOME}/bin/"

# Run sleep infinity so that the dev container stays alive
CMD [ "sleep", "infinity" ]
