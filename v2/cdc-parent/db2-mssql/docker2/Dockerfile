FROM ibmcom/db2@sha256:220c15de7eba67e97d6a3db0f08ae171d246acdfb18d7fd109ce43652f97676e

ENV LICENSE=accept
ENV DB2INST1_PASSWORD=password
ENV DBNAME=testdb

COPY ./helpers/entrypoint.sh /home/entrypoint.sh
CMD "db2start"
CMD ["sh", "home/entrypoint.sh"]