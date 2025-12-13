FROM daskdev/dask:latest

USER root

COPY requirements.txt /tmp/requirements.txt

# ставим через pip всё, КРОМЕ netcdf4/pyarrow (их ставим через conda)
RUN grep -v -E '^(netcdf4|pyarrow|fastparquet)([<=>].*)?$' /tmp/requirements.txt > /tmp/req_pip.txt \
 && pip install --no-cache-dir -r /tmp/req_pip.txt

# научный стек + совместимые бинарники — через conda-forge
RUN conda install -y -c conda-forge \
      "libstdcxx-ng>=13.2" \
      "hdf5>=1.14" \
      "libnetcdf>=4.9" \
      netcdf4 \
      h5netcdf \
      h5py \
      pyarrow \
 && conda clean -afy

WORKDIR /opt/app
