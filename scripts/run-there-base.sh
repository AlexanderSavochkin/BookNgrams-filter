yum install git

yum install -y http://dl.bintray.com/sbt/rpm/sbt-0.13.5.rpm

mkdir ~/ngrams

cd ngrams

git clone https://github.com/AlexanderSavochkin/BookNgrams-filter

cd BookNgrams-filter

sbt clean compile test assembly

cp target/scala-2.10/ngramsfilter-assembly-1.0.jar ~/
cp data/* ~/

~/spark/bin/spark-submit --class net.chwise.dataaquisition.textmining.NGramFilter ~/google-n-grams-filter-for-chwise-net_2.10-1.0.jar -t ~/list-compoundnames.txt -g list-ngrams-s3-pathes.txt -o hdfs://chemical-ngrams.tsv

