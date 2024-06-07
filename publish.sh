set -ex
for spark_version in 3.0.0 3.1.1 3.2.0 3.3.1 3.4.1 3.5.1
do
  sbt  -DsparkVersion=$spark_version version clean +publishLocal +publishSigned
done
