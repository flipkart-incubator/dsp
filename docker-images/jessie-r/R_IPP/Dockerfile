FROM 4e81032c564f
ADD generate_sources.sh /app
RUN bash /app/generate_sources.sh
RUN apt-get update
RUN apt-get install --yes curl
RUN apt-get install --yes libcurl4-openssl-dev
RUN export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig
RUN apt-get install --yes make
RUN apt-get install --yes gcc
RUN apt-get install --yes gfortran
WORKDIR /app
ADD install_packages.R /app
RUN Rscript /app/install_packages.R


