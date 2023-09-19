FROM ubuntu:latest

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y --no-install-recommends \
git \
python3-setuptools \ 
python3-pip \
python3-dev \
python3-venv \
raxml \
mrbayes \
&& \
apt-get clean && \
rm -rf /var/lib/apt/lists/*
EXPOSE 8000
#ENV NODE_ENV production
CMD ["executable","param1","param2",...]