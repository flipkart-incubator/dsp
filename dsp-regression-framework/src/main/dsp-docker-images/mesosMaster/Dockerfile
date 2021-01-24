FROM debian:stretch
WORKDIR /app
ADD userCreate.sh /app
ADD bootstart.sh /app
RUN bash /app/userCreate.sh
RUN bash /app/bootstart.sh
ADD EntryPoint.sh /app/EntryPoint.sh
RUN chmod +x /app/EntryPoint.sh
ENTRYPOINT ["/app/EntryPoint.sh"]