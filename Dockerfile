# Dockerfile for installation of HP2Net framework. All the lines related to quartet maxcut are commented due to the software
# being unavailable
FROM ubuntu:latest

WORKDIR /app

COPY . /app/

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y --no-install-recommends \
git \
python3-setuptools \ 
python3-pip \
python3-dev \
python3-venv \
raxml \
mrbayes \
openjdk-17-jre \
openjdk-17-jdk \
build-essential \
wget \
r-base \
&& \
apt-get clean
RUN echo 'export PATH=$PATH:/app/julia-1.9.3/bin' >> ~/.bashrc 
RUN wget https://julialang-s3.julialang.org/bin/linux/x64/1.9/julia-1.9.3-linux-x86_64.tar.gz && \
tar -zxvf julia-1.9.3-linux-x86_64.tar.gz && \
RUN git clone http://www.stat.wisc.edu/~ane/bucky.git && \
cd bucky/src && make && cp bucky mbsum /usr/local/bin && ../..
#RUN http://research.haifa.ac.il/~ssagi/software/QMCN.tar.gz
#RUN mkdir -p QMC
#RUN tar -zxvf QMCN.tar.gz -C QMC
#RUN cd QMC
#RUN cp quartet-agreement-Linux-64 genTreeAndQuartets-Linux-64 find-cut-Linux-64 /usr/local/bin
RUN git clone https://github.com/smirarab/ASTRAL.git && \
cd ASTRAL && \
sed -i 's/1.6/1.7/g' make.sh  && \
chmod a+x make.sh && \
cp astral.*.jar /usr/local/bin
RUN mkdir -p /usr/local/bin/lib
RUN cp lib/* /usr/local/bin/lib
RUN cd ../..
RUN wget https://github.com/iqtree/iqtree2/releases/download/v2.1.3/iqtree-2.1.3-Linux.tar.gz
RUN mkdir -p iqtree
RUN tar -zxvf iqtree-2.1.3-Linux.tar.gz -C iqtree 
RUN cd iqtree
RUN iqtree-2.1.3-Linux/bin/iqtree2 /usr/local/bin
RUN cd ..
RUN wget https://github.com/NakhlehLab/PhyloNet/releases/latest/download/PhyloNet.jar
RUN cp PhyloNet.jar /usr/local/bin
RUN rm -rf ASTRAL iqtree bucky /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip && \
pip install --no-cache-dir parsl && \
pip install --no-cache-dir parsl[monitoring] && \
pip install --no-cache-dir biopython && \
pip install --no-cache-dir pandas
EXPOSE 8000
#ENV NODE_ENV production
CMD ["python3", "parsl_workflow.py"]
