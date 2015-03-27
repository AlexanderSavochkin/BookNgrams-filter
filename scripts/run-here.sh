#!/bin/sh

#Before running this script you should define following env varaibles:
#
# SAPRK_HOME <- path to spark directory
# NUMSLAVES <- number of slaves in cluster
# AWS_ACCESS_KEY_ID <- amazon key
# AWS_SECRET_ACCESS_KEY <- amazon secret key
# KEYPATH <- /home/asavochkin/Work/Projects/ChWiSe/spark-cluster.pem

$SPARK_HOME/ec2/spark-ec2 -k spark-cluster --zone=us-east-1d  -i $KEYPATH -s $NUMSLAVES --copy-aws-credentials  launch test-spark-cluster

MASTER=`$SPARK_HOME/ec2/spark-ec2 -k spark-cluster -i /home/asavochkin/Work/Projects/ChWiSe/spark-cluster.pem get-master test-spark-cluster`


echo ****************************************************************
echo MASTER: $MASTER
echo ****************************************************************


echo "#!/bin/sh" > run-there.sh
echo "export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> run-there.sh
echo "export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> run-there.sh

cat run-there-base.sh >> run-there.sh

scp -i $KEYPATH run-there.sh root@$MASTER:

$SPARK_HOME/ec2/spark-ec2 -k spark-cluster -i /home/asavochkin/Work/Projects/ChWiSe/spark-cluster.pem login test-spark-cluster
