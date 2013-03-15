
Brooklyn MapR Roll-out
======================

This project contains Brooklyn entities for the components of a MapR M3 system,
and a sample application which deploys it to Amazon.


### Compile

To compile brooklyn-mapr, simply `mvn clean install` in the project root.


### Run

To run it, either:

* Download and install the `brooklyn` CLI tool from http://brooklyncentral.github.com/ and run in the project root:

  export BROOKLYN_CLASSPATH=target/brooklyn-mapr-0.1.0-SNAPSHOT.jar 
  brooklyn launch -a io.cloudsoft.mapr.MyM3App -l aws-ec2:us-east-1

* Grab all dependencies (using maven, or in your favourite IDE) and run the static `main` in `io.cloudsoft.mapr.MyM3App`

After about 20 minutes, it should print out the URL of the MapR master node and the Brooklyn console.  
You must manually accept the license in MapR (credentials defined in MyM3App), 
and then manually inform Brooklyn you have done so (effector setLicenseApproved on master),
then the cluster will continue to set up (another 2 minutes or so).

Once fully booted, you can resize (scale out) the worker cluster, stop nodes, and see a few sensors.
As an exercise to the reader, add new sensors with the metrics you care about, and perhaps add a
policy to automatically scale out.  (See other Brooklyn examples for an illustration.)


### Setup

In both cases you'll need AWS credentials in `~/.brooklyn/brooklyn.properties`:

  brooklyn.jclouds.aws-ec2.identity=AKXXXXXXXXXXXXXXXXXX
  brooklyn.jclouds.aws-ec2.credential=secret01xxxxxxxxxxxxxxxxxxxxxxxxxxx

Most other clouds should work too, with minor variations to the code (in particular the disk setup in MyM3App),
as will fixed IP machines (bare-metal/byon).  MaaS clouds (metal-as-a-service) are in development, over at jclouds.org.


### Finally

This software is (c) 2012 Cloudsoft Corporation, released as open source under the Apache License v2.0.

Any questions drop a line to brooklyn-users@googlegroups.com !

