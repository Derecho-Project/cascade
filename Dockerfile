# syntax=docker/dockerfile:1

FROM highorderbits/derecho-lib:latest AS base

# Install apt-packaged dependencies
RUN apt-get update && apt-get install -y \
    libreadline-dev \
    ragel \
    libboost-dev \
    libfuse3-dev \
    python3

# Base stage for building dependencies
FROM base AS dep-build-base

# Install apt packages only needed for building dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip

# Copy in the "install preqrequisites" scripts
COPY scripts/prerequisites /prerequisites
WORKDIR /prerequisites

# Stage for building the required dependencies for core Cascade features
# Runs the install scripts to install them in /usr/local/
FROM dep-build-base AS required-dep-build
RUN ./install-hyperscan.sh
RUN ./install-rpclib.sh
RUN ./install-libwsong.sh
RUN ./install-boolinq.sh

# Stage for building the optional dependencies that are only used for some example applications
# Runs a different set of install scripts and installs a few more apt packages
FROM dep-build-base AS optional-dep-build
RUN apt-get update && apt-get install -y \
    libopencv-dev \
    python3-opencv \
    libopenblas-dev

RUN ./install-ann.sh
RUN ./install-cppflow.sh
RUN ./install-libtorch.sh cpu
RUN ./install-mxnet-src.sh
RUN ./install-tensorflow.sh 2.9.3 cpu

# Just for CascadeChain: a stage that builds WanAgent
FROM required-dep-build AS wanagent-build
# clone and install the private WanAgent repo
ADD git@github.com:derecho-project/wanagent.git wanagent
RUN cd wanagent && mkdir build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && cmake --build . -j $(nproc) \
    && cmake --install .

# Final development image stage for Cascade with the optional dependencies.
# Declared before the end of the file so it won't be the default.
FROM base AS cascade-dev-all
# Install these apt-packaged optional dependencies again
RUN apt-get update && apt-get install -y \
    libopencv-dev \
    python3-opencv \
    libopenblas-dev
# Copy everything in the dependency builder stages' /usr/local/ to get the compiled libraries
COPY --from=required-dep-build /usr/local/ /usr/local/
COPY --from=optional-dep-build /usr/local/ /usr/local/
COPY --from=wanagent-build /usr/local/ /usr/local/

# Copy in all the Cascade source code
WORKDIR /cascade
COPY . .

# Build initially in Debug mode, since this image is for development
RUN mkdir build-Debug && cd build-Debug \
    && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. \
    && cmake --build . -j $(nproc)

# Set the default command to a shell for interactive development
CMD ["/bin/bash"]

# Final development image stage with default dependencies (not optional ones)
FROM base AS cascade-dev

# Copy everything in the dependency builder stages' /usr/local/ to get the compiled libraries
COPY --from=required-dep-build /usr/local/ /usr/local/
COPY --from=wanagent-build /usr/local/ /usr/local/

# Copy in all the Cascade source code
WORKDIR /cascade
COPY . .

# Build initially in Debug mode, since this image is for development
RUN mkdir build-Debug && cd build-Debug \
    && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. \
    && cmake --build . -j $(nproc)

# Set the default command to a shell for interactive development
CMD ["/bin/bash"]

