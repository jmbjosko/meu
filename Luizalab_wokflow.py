#--------------
# Author: J. M. B. Josko
# --------------
# Preamble - Loading required packages and set parameters
# --------------

import luigi
from luigi.file import LocalTarget
import subprocess

#------------
# to run me: python Luizalab_wokflow.py KinesisTask --local-scheduler
#------------

#------------
# STEP 1 - Kinessis Stream consumer
#------------

class KinesisTask(luigi.Task):

    # Define the files generated
    def output(self):
        return luigi.LocalTarget("stream.csv")

    # Run kinesis consumer
    def run(self):       
        subprocess.call("kinConsumer.py", shell=True)

#------------
# STEP 2 - Order + Stream Production
#------------
class RTask(luigi.Task):

    # Depends on Kinesis task
    def requires(self):
        return [KinesisTask()]
    
    def output(self):
        return LocalTarget("fact_ord.csv")
    
    def run(self):
        subprocess.call('Rscript Rtask.R',shell=True)

#------------
# Run Workflow
#------------

if __name__ == '__main__':
    luigi.run()    
