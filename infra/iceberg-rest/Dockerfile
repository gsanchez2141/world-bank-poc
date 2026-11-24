# apache/iceberg-rest-fixture:1.10.1
# Pinned 1.x tag, not latest. The contract minimum is 1.8.1.
FROM apache/iceberg-rest-fixture:1.10.1

USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar /usr/lib/iceberg-rest/postgresql-42.7.5.jar
COPY entrypoint.sh /usr/lib/iceberg-rest/entrypoint.sh
RUN chmod 755 /usr/lib/iceberg-rest/entrypoint.sh \
    && chmod 644 /usr/lib/iceberg-rest/postgresql-42.7.5.jar \
    && chown iceberg:iceberg /usr/lib/iceberg-rest/entrypoint.sh /usr/lib/iceberg-rest/postgresql-42.7.5.jar
USER iceberg
ENTRYPOINT ["/usr/lib/iceberg-rest/entrypoint.sh"]
