FROM debian:jessie
WORKDIR /app
ADD userCreate.sh /app
ADD bootstart.sh /app
ADD azkaban_exec.properties /app
ADD azkaban_web.properties /app
ADD hadoop-infra-hadoop-conf.postinst /app
RUN bash /app/userCreate.sh
RUN bash /app/bootstart.sh
ADD EntryPoint.sh /app/EntryPoint.sh
RUN chmod +x /app/EntryPoint.sh
ENTRYPOINT ["/app/EntryPoint.sh"]