# syntax=docker/dockerfile:1

FROM highorderbits/derecho-lib:latest AS base

# Install apt-packaged dependencies
RUN apt-get update && apt-get install -y \
    libreadline-dev \
    ragel \
    libfuse3-dev

# Stage for building dependencies, so their sources aren't included in the final image
FROM base AS dep-build

# Run all of the "install prerequisite" scripts, which
# compile and install the libraries into /usr/local/
COPY scripts/prerequisites /prerequisites
WORKDIR /prerequisites
RUN ./install-ann.sh
RUN ./install-boolinq.sh
RUN ./install-cppflow.sh
RUN ./install-hyperscan.sh
RUN ./install-libtorch.sh
RUN ./install-libwsong.sh
RUN ./install-mxnet-src.sh
RUN ./install-opencv.sh
RUN ./install-rpclib.sh
RUN ./install-tensorflow.sh

# Just for CascadeChain: clone and install the private WanAgent repo
ADD git@github.com:derecho-project/wanagent.git wanagent
RUN cd wanagent && mkdir build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && cmake --build . -j $(nproc) \
    && cmake --install .

FROM base AS cascade-dev

# Copy everything in the dep-build stage's /usr/local/ to get the compiled libraries
COPY --from=dep-build /usr/local/ /usr/local/

# Copy in all the Cascade source code
WORKDIR /cascade
COPY . .

# Build initially in Debug mode, since this image is for development
RUN mkdir build-Debug && cd build-Debug \
    && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. \
    && cmake --build . -j $(nproc)

# Set the default command to a shell for interactive development
CMD ["/bin/bash"]