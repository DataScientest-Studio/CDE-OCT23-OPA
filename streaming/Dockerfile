# Start with the official Apache Airflow image
FROM apache/airflow:2.8.0

# Switch to root user to install OS-level packages
USER root

# Install necessary OS-level packages
RUN apt-get update && \
    apt-get install -y wget curl gnupg procps && \
    apt-get clean

# Install OpenJDK 11 from AdoptOpenJDK or Eclipse Temurin
RUN wget -O /tmp/openjdk11.tar.gz https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.19+7/OpenJDK11U-jdk_x64_linux_hotspot_11.0.19_7.tar.gz && \
    mkdir -p /opt/openjdk && \
    tar -xzf /tmp/openjdk11.tar.gz -C /opt/openjdk && \
    rm /tmp/openjdk11.tar.gz

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/openjdk/jdk-11.0.19+7
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Switch to airflow user to install Python packages
USER airflow

# Install Spark and any other necessary Python packages
RUN pip install pyspark

# Switch back to root to copy files and set permissions
USER root

# Ensure the build context is correctly set and paths align
WORKDIR /opt/airflow

# Copy entrypoint script to the appropriate directory
COPY script/entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh

# Copy all DAGs and scripts to their appropriate directories
COPY dags/ /opt/airflow/dags/
COPY script/ /opt/airflow/script/

# Switch back to airflow user for running Airflow commands
USER airflow

# Set entrypoint script
ENTRYPOINT ["/opt/airflow/entrypoint.sh"]

# Default command
CMD ["webserver"]
