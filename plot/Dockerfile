FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC 

##############################
# Install Dependencies
#############################

# Directory
RUN mkdir /data

# Install essential
RUN apt-get update && apt-get install -y \
	build-essential \
	curl \
	python3.8-dev python3-pip \
        fontconfig
RUN apt-get install -y ttf-mscorefonts-installer

RUN fc-cache -fv
RUN fc-list

COPY . /data/plot

RUN pip3 install -r /data/plot/requirements.txt
