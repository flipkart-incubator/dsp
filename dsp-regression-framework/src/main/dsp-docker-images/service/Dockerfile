FROM debian:jessie
WORKDIR /app
ADD userCreate.sh /app
ADD bootstart.sh /app
ADD *.fkinternal.com.cer /app
ADD ipp-dsp-service.postinst /app
RUN bash /app/userCreate.sh
RUN bash /app/bootstart.sh
ADD setup.sh /app
RUN chmod +x /app/setup.sh
ENTRYPOINT ["/app/setup.sh"]
