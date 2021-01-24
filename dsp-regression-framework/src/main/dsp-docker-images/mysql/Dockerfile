FROM debian:jessie
WORKDIR /app
ADD userCreate.sh /app
RUN bash /app/userCreate.sh
ADD bootstart.sh /app
ADD ipp_azkaban_2019-12-06.sql /app

RUN chmod +x /app/bootstart.sh
RUN bash /app/bootstart.sh
ADD EntryPoint.sh /app/EntryPoint.sh
RUN chmod +x /app/EntryPoint.sh
ENTRYPOINT ["/app/EntryPoint.sh"]