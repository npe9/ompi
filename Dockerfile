FROM ubuntu:22.04 AS base

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV OMPI_VERSION=5.0.3
ENV OMPI_DIR=/opt/openmpi-${OMPI_VERSION}
ENV PATH=${OMPI_DIR}/bin:${PATH}
ENV LD_LIBRARY_PATH=${OMPI_DIR}/lib:${LD_LIBRARY_PATH}

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    file \
    git \
    autoconf \
    automake \
    libtool \
    flex \
    python3 \
    python3-pip \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Download and extract OpenMPI
RUN wget https://download.open-mpi.org/release/open-mpi/v5.0/openmpi-${OMPI_VERSION}.tar.gz \
    && tar xzf openmpi-${OMPI_VERSION}.tar.gz \
    && rm openmpi-${OMPI_VERSION}.tar.gz

# Build and install OpenMPI
WORKDIR /openmpi-${OMPI_VERSION}
RUN ./configure --prefix=${OMPI_DIR} \
    --with-hwloc=internal \
    --with-libevent=internal \
    && make -j$(nproc) \
    && make install \
    && make clean

# Clean up
WORKDIR /
RUN rm -rf /openmpi-${OMPI_VERSION}

# Build lithe
FROM base AS lithe-builder
WORKDIR /workspace
RUN apt-get update && \
    apt-get install -y build-essential cmake && \
    git clone https://github.com/klueska/lithe.git && \
    cd lithe && \
    mkdir build && cd build && \
    cmake .. && \
    make -j$(nproc) && \
    make install

# Build parlib
FROM base AS parlib-builder
WORKDIR /workspace
RUN apt-get update && \
    apt-get install -y build-essential && \
    git clone https://github.com/klueska/parlib.git && \
    cd parlib && \
    ./configure && \
    make -j$(nproc) && \
    make install

# Build OpenMP with Lithe support
FROM base AS openmp-builder
COPY --from=lithe-builder /usr/local/lib/liblithe.* /usr/local/lib/
COPY --from=lithe-builder /usr/local/include/lithe /usr/local/include/lithe
WORKDIR /workspace/llvm-openmp/runtime
RUN apt-get update && \
    apt-get install -y cmake clang && \
    mkdir -p build && cd build && \
    cmake .. \
      -DCMAKE_C_COMPILER=clang \
      -DCMAKE_CXX_COMPILER=clang++ \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DLIBOMP_THREADING_BACKEND=lithe \
      -DLIBOMP_USE_LITHE=ON \
      -DLITHE_INCLUDE_DIR=/usr/local/include/lithe \
      -DLITHE_LIBRARY=/usr/local/lib/liblithe.so && \
    make -j$(nproc) && \
    make install

# Build miniFE
FROM base AS minife-builder
COPY --from=parlib-builder /usr/local/lib/libparlib.* /usr/local/lib/
COPY --from=parlib-builder /usr/local/include/parlib /usr/local/include/parlib
COPY --from=parlib-builder /usr/local/lib/pkgconfig/parlib.pc /usr/local/lib/pkgconfig/
COPY --from=lithe-builder /usr/local/lib/liblithe.* /usr/local/lib/
COPY --from=lithe-builder /usr/local/include/lithe /usr/local/include/lithe
COPY --from=openmp-builder /usr/local/lib/libomp* /usr/local/lib/
COPY --from=openmp-builder /usr/local/include/omp* /usr/local/include/
COPY --from=ompi-builder /usr/local/bin/mpi* /usr/local/bin/
COPY --from=ompi-builder /usr/local/lib/libmpi* /usr/local/lib/
WORKDIR /workspace
RUN git clone https://github.com/Mantevo/miniFE.git && \
    cd miniFE && \
    mkdir build && cd build && \
    cmake .. \
        -DCMAKE_C_COMPILER=mpicc \
        -DCMAKE_CXX_COMPILER=mpicxx \
        -DCMAKE_BUILD_TYPE=Release \
        -DENABLE_OPENMP=ON \
        -DENABLE_MPI=ON && \
    make -j$(nproc) 