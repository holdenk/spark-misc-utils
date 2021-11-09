set -ex
for spark_version in 2.4.0 3.0.0 3.1.1 3.2.0
do
  sbt  -DsparkVersion=$spark_version version clean +publishLocal +publishSigned
done
