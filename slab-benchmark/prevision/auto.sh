# Postgres Docker
docker pull postgres
docker run -p15432:5432 --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres

sleep 10

# Virtualenv
virtualenv -p $(which python2) venv2
source ./venv2/bin/activate
pip install -r requirements2.txt
deactivate

python3 -m venv venv3
source ./venv3/bin/activate
pip install -r requirements3.txt
deactivate

# Generate CSV
bash ./generator/auto_gen_csv.sh

# Convert CSV to Numpy npy files
bash ./generator/auto_conv_csv_to_npy.sh

# Convert NumPy npy to Dask HDF5 files
bash ./generator/auto_conv_npy_to_hdf5.sh

# Convert dense CSV to COO CSV for SciDB 
bash ./generator/auto_conv_densecsv_to_coocsv.sh

# Convert CSV to sequence files 
bash ./generator/auto_conv_csv_to_sf.sh

# Convert CSV to SystemDS binary format
bash ./generator/auto_conv_csv_to_bin.sh

# Generate PreVision Format

