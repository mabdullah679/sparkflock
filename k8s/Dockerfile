# Spark image for minikube: stock Apache Spark + the S3A/Postgres jars baked in
# (so 4 executor pods never each download 280 MB from Maven at start-up) + the job.
FROM apache/spark:3.5.6-python3
USER root
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/postgresql-42.7.4.jar
COPY spark/job.py spark/schema.sql /opt/app/
USER 185
