FROM ubuntu:18.04 AS base

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    autoconf \
    automake \
    libtool \
    pkg-config \
    python3 \
    python3-pip \
    flex \
    && rm -rf /var/lib/apt/lists/*

# Build parlib
FROM base AS parlib-builder
WORKDIR /build
RUN git clone https://github.com/klueska/parlib.git && \
    cd parlib && \
    ./bootstrap && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    ldconfig && \
    echo "Checking parlib library contents:" && \
    ls -l /usr/local/lib/libparlib.* && \
    file /usr/local/lib/libparlib.* && \
    nm /usr/local/lib/libparlib.a | grep uthread_init || true

# Build lithe
FROM base AS lithe-builder
WORKDIR /build
RUN git clone https://github.com/klueska/lithe.git && \
    cd lithe && \
    ./bootstrap && \
    LDFLAGS="-L/usr/local/lib" CPPFLAGS="-I/usr/local/include" ./configure --with-parlib=/usr/local && \
    make && \
    make install

# Build OpenMPI with Lithe support
FROM base AS ompi-builder
WORKDIR /build
COPY ./ompi /build/ompi

# Autogen step
FROM ompi-builder AS ompi-autogen
RUN cd ompi && ./autogen.pl

# Configure step
FROM ompi-autogen AS ompi-configure
RUN cd ompi && ./configure --with-hwloc=internal \
                          --with-libevent=internal \
                          --with-lithe=/usr/local \
                          --with-parlib=/usr/local

# Build step
FROM ompi-configure AS ompi-build
RUN cd ompi && make -j$(nproc)

# Install step
FROM ompi-build AS ompi-install
RUN cd ompi && make install

# Final stage
FROM base
COPY --from=ompi-install /usr/local/bin/mpi* /usr/local/bin/
COPY --from=ompi-install /usr/local/lib/libmpi* /usr/local/lib/
COPY --from=ompi-install /usr/local/lib/libpmix* /usr/local/lib/
COPY --from=ompi-install /usr/local/lib/libopen-pal* /usr/local/lib/
COPY --from=ompi-install /usr/local/lib/libopal* /usr/local/lib/
COPY --from=ompi-install /usr/local/lib/libompi* /usr/local/lib/
COPY --from=ompi-install /usr/local/lib/libevent* /usr/local/lib/
COPY --from=ompi-install /usr/local/lib/libhwloc* /usr/local/lib/
COPY --from=ompi-install /usr/local/bin/prterun /usr/local/bin/
COPY --from=ompi-install /usr/local/lib/libprrte* /usr/local/lib/
COPY --from=ompi-install /usr/local/share /usr/local/share
COPY --from=ompi-install /usr/local/lib/openmpi /usr/local/lib/openmpi
COPY --from=ompi-install /usr/local/lib/pmix /usr/local/lib/pmix

# Add non-root user
RUN useradd -ms /bin/bash mpiuser

# Set up runtime environment
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
ENV PATH=/usr/local/bin:$PATH

# Set working directory for the application
WORKDIR /app

# Copy application files
COPY . .

# Set permissions for the user
RUN chown -R mpiuser:mpiuser /app

# Switch to non-root user
USER mpiuser

# Set the entrypoint
ENTRYPOINT ["mpirun"] 