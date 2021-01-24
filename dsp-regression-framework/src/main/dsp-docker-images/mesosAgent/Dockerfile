FROM debian:stretch
WORKDIR /app
ADD userCreate.sh /app
ADD dindImage.sh /app
ADD dockerDaemon.sh /app
ADD mesosAgentSetup.sh /app
RUN bash /app/userCreate.sh
RUN bash /app/dindImage.sh
RUN bash /app/dockerDaemon.sh
RUN bash /app/mesosAgentSetup.sh
ADD EntryPoint.sh /app/EntryPoint.sh
RUN chmod +x /app/EntryPoint.sh
ENTRYPOINT ["/app/EntryPoint.sh"]