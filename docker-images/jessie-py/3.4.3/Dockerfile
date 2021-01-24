# Use an official Python runtime as a parent image
FROM debian:jessie-slim
# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD generate_sources.sh /app
ADD requirements.txt /app

# Install any needed packages
RUN mkdir /usr/share/man/man1/
RUN touch /usr/share/man/man1/java.1.gz.dpkg-tmp
RUN bash /app/generate_sources.sh

RUN apt-get update
RUN apt-get --yes --allow-unauthenticated install sudo
RUN apt-get install --yes --allow-unauthenticated oracle-j2sdk1.8
RUN apt-get install --yes --allow-unauthenticated apt-utils
RUN apt-get install --yes --allow-unauthenticated pkg-config

RUN apt-get install --yes --allow-unauthenticated gcc gfortran python-dev libopenblas-dev liblapack-dev cython
RUN apt-get install --yes --allow-unauthenticated python3-pip
RUN apt-get install --yes --allow-unauthenticated python3-thrift-binding

RUN pip3 install -r requirements.txt
