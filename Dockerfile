# Dockerfile for installation of HP2Net framework. All the lines related to quartet maxcut are commented due to the software
# being unavailable
FROM python:3.12-slim-bullseye
LABEL version="1.0" maintainer="Rafael Terra <rafaelstjf@gmail.com>"
WORKDIR /app
COPY . /app/
ENV DEBIAN_FRONTEND=noninteractive 
ENV TZ=Etc/UTC

# Install essential packages
RUN apt-get update && \
    apt-get install -y \
        software-properties-common \
        git \
        build-essential \
        wget \
        ca-certificates \
        python3-setuptools \
        python3-pip \
        python3-dev \
        python3-venv \
        raxml \
        mrbayes \
        openjdk-17-jre \
        openjdk-17-jdk \
        r-base && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Julia
RUN wget --no-check-certificate https://julialang-s3.julialang.org/bin/linux/x64/1.9/julia-1.9.3-linux-x86_64.tar.gz && \
    tar -zxvf julia-1.9.3-linux-x86_64.tar.gz -C /app && \
    rm julia-1.9.3-linux-x86_64.tar.gz
ENV PATH=$PATH:/app/julia-1.9.3/bin

# Clone and build Bucky
# RUN git clone https://pages.stat.wisc.edu/~ane/bucky.git/ && \
#     cd bucky/src && \
#     sed -i 's/unordered_map/boost::unordered_map/g' TGM.h && \
#     make && \
#     mv bucky mbsum /usr/local/bin

# Install Bucky and mbsum
RUN mv /app/tools/bucky /usr/local/bin && \
    chmod a+x /usr/local/bin/bucky
RUN mv /app/tools/mbsum /usr/local/bin && \
    chmod a+x /usr/local/bin/mbsum
# Install ASTRAL
RUN mkdir -p /usr/local/bin/lib
RUN git clone https://github.com/smirarab/ASTRAL.git && \
    cd ASTRAL && \
    sed -i 's/1.6/1.7/g' make.sh  && \
    chmod a+x make.sh && \
    ./make.sh && \
    cd Astral && \
    mv astral*.jar Astral.jar && \
    cp *.jar /usr/local/bin && \
    cp lib/* /usr/local/bin/lib

# Install IQ-TREE
RUN wget https://github.com/iqtree/iqtree2/releases/download/v2.1.3/iqtree-2.1.3-Linux.tar.gz && \
    tar -zxvf iqtree-2.1.3-Linux.tar.gz -C /app && \
    rm iqtree-2.1.3-Linux.tar.gz && \
    mv /app/iqtree-2.1.3-Linux/bin/iqtree2 /usr/local/bin

# Install PhyloNet
RUN wget https://github.com/NakhlehLab/PhyloNet/releases/latest/download/PhyloNet.jar && \
    cp PhyloNet.jar /usr/local/bin

# Install Quartet Maxcut
RUN mv /app/tools/find-cut-Linux-64 /usr/local/bin && \
    chmod a+x /usr/local/bin/find-cut-Linux-64
# Clean up
RUN rm -rf ASTRAL iqtree bucky /var/lib/apt/lists/*

# Install Python packages
RUN python3 -m pip install  --upgrade pip && \
    python3 -m pip install -r requirements.txt

RUN julia -e 'using Pkg; Pkg.add(Pkg.PackageSpec(name="PhyloNetworks", version="0.16.4"))'
RUN julia -e 'using Pkg; Pkg.add(["RCall", "PhyloPlots", "CSV"])'
# Set the default command
ENTRYPOINT ["python3", "parsl_workflow.py"]
