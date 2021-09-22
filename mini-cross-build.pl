#!/usr/bin/perl
use File::Slurp;
use strict;
use warnings;
my @spark_versions = (
    "2.4.0",
    "3.0.0",
    "3.1.1", # Special 3.1.0 was not properly released.
    );
# Backup the build file
`cp build.sbt build.sbt_back`;
# Get the original version
my $input = read_file("build.sbt");
foreach my $spark_version (@spark_versions) {
    print "Next spark version ".$spark_version;
    print "\nbuilding\n";
    # Publish local first so kafka sub project can resolve
    print "\nGoing to run: sbt  -DsparkVersion=$spark_version version clean +publishLocal\n";
    print `sbt  -DsparkVersion=$spark_version version clean test +publishLocal`;
    print "\nGoing to run: sbt  -DsparkVersion=$spark_version version clean test +publishSigned\n";
    print `sbt  -DsparkVersion=$spark_version version clean test +publishSigned`;
    print "\nbuilt\n";
}
