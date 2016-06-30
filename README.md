# EC3-migration
Script to perform migration operations to virtual clusters deployed with EC3.

EC3 is in charge of cloning the cluster. Checkpointing of the applications is achieved by BLCR and SLURM, and files are saved in Amazon S3. It requires sshpass to be installed where the script is run, to work properly.

Example of invocation:
python migrate.py 158.42.105.67 --name mycluster --destination ec2 

More details about EC3 can be found in: 
- https://github.com/grycap/ec3 
- http://www.grycap.upv.es/ec3
