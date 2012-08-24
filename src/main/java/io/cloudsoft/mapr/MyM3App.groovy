package io.cloudsoft.mapr

import io.cloudsoft.mapr.m3.AbstractM3Node
import io.cloudsoft.mapr.m3.M3Disks
import brooklyn.entity.basic.AbstractApplication
import brooklyn.launcher.BrooklynLauncher;
import brooklyn.location.basic.LocationRegistry;

/** starts an M3 cluster in AWS. login as username 'ubuntu', password 'm4pr'. */
public class MyM3App extends AbstractApplication {

    M3 m3 = new M3(this);
    
    {
        // uncomment the following line if you accept the license, otherwise you will be prompted to accept in the MapR GUI
//        m3.setConfig(LICENSE_ACCEPTED, true);

        // uncomment the following line if you have a license, otherwise you will be prompted to accept the default in the MapR GUI
//        m3.setConfig(LICENSE_KEY_FILE, "/tmp/myM3Key);
        
        // disks can be set specifically on any entity; 
        // setting on the root will provide a default which is inherited everywhere 
        m3.setConfig(AbstractM3Node.DISK_SETUP_SPEC,
            M3Disks.builder().
                disks(
                    "/mnt/mapr-storagefile1", 
                    "/mnt/mapr-storagefile2").
                commands(
                    "sudo truncate -s 20G /mnt/mapr-storagefile1",
                    "sudo truncate -s 10G /mnt/mapr-storagefile2").
                    // above seems quick and portable
                    // hdparm --fallocate 20000000 /mnt/mapr-storagefile1   -- also good, but needs ext4
                    // "dd if=/dev/zero of=/mnt/mapr-storagefile1 bs=512M count=40",  -- the slow, standard way
                build() );
    }

    // can start in AWS by running this -- or use brooklyn CLI/REST for most clouds, or programmatic/config for set of fixed IP machines
    public static void main(String[] args) {
        MyM3App app = new MyM3App();
        BrooklynLauncher.manage(app, 8081);
        app.start( new LocationRegistry().getLocationsById(["aws-ec2:us-east-1"]) );
    }
    
}
