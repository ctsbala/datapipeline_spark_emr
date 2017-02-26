'''
Created on Feb 26, 2017

@author: ctsbala

'''
import boto3
import luigi
from luigi.s3 import S3Target


class FileToS3(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget("C:/localdir/lineorder_shipment_agg.py")
        
class CopyToS3(luigi.Task):
    
    def requires(self):
        FileToS3()
    
    def output(self):
        return S3Target("s3n://aws_bucket/pySparkSrc/lineorder_shipment_agg.py")
    
    def run(self):
        s3=boto3.resource('s3')
        
        buckets = list(s3.buckets.all())
        
        if not any(x.name=="aws_bucket" for x in buckets):
            s3.create_bucket(Bucket='aws_bucket',\
                             CreateBucketConfiguration={'LocationConstraint': 'us-east-1'} \
                             )
        with open('C:/localdir/lineorder_shipment_agg.py', 'rb') as data:
            s3.Bucket('aws_bucket').put_object(Key='pySparcSrc/lineorder_shipment_agg.py', Body=data)

    
class StartEMRCluster(luigi.Task):
    client = boto3.client('emr', region_name='us-east-1')

    def requires(self):
        #all the input files
        #all the application steps as individual files
        CopyToS3()

    
    def output(self):
        return S3Target("s3n://aws_bucket/SparkLineOrderout")

    def run(self):
    
        response = self.client.run_job_flow(
            Name='process-lineorders',
            #LogUri='string',
            #AdditionalInfo='string',
            #AmiVersion='string',
            ReleaseLabel='emr-5.3.1',
            Instances={'MasterInstanceType': 'm3.xlarge',
                      'SlaveInstanceType': 'm3.xlarge',
                      'InstanceCount': 3,
                      'Ec2KeyName': 'sparkec2',
                      'KeepJobFlowAliveWhenNoSteps': False,
                      'TerminationProtected': False,
                      'EmrManagedMasterSecurityGroup': 'sg-00001',
                      'EmrManagedSlaveSecurityGroup': 'sg-00002',
                      'Placement': {
                                'AvailabilityZone': 'us-east-1d'
                        },
                      },
            Steps=[
                        {
                            'Name': 'WordCount',
                            'ActionOnFailure': 'CONTINUE',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                "Args": [
                                      "spark-submit",
                                      "--deploy-mode",
                                      "cluster",
                                      "s3://aws_bucket/PySparkSrc/lineorder_shipment_agg.py"
                                    ]
                            }
                        },
            ],
            Applications=[
                {
                    'Name': 'spark'
                },
                {         
                    'Name':'Zeppelin'
                },
                {
                    'Name':'Ganglia'
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            ScaleDownBehavior='TERMINATE_AT_INSTANCE_HOUR'
        )

        waiter = self.client.get_waiter('cluster_running')
        waiter.wait(
                        ClusterId='process-lineorders'
                    )

        waiter = self.client.get_waiter('cluster_terminated')
        waiter.wait(
                        ClusterId='process-lineorders'
                    )
        
        print (response)


if __name__ == "__main__":
    luigi.run(main_task_cls=StartEMRCluster)        