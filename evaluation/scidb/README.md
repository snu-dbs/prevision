Docker starting command

```bash
sudo docker run --name tilepack-scidb-exp -it --shm-size=30gb -v /mnt/nas:/nas -v /mnt/ssd/tilepack/scidb/dbpath:/dbpath rvernica/scidb:19.11
```

`scidb_docker_empty.tar.gz` is an image that I changed a user running SciDB from root to scidb.
SciDB makes an error related to MPI when it is run by root user.
