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
        // disks can be set specifically on any entity; 
        // setting on the root will provide a default which is inherited everywhere 
        m3.setConfig(AbstractM3Node.DISK_SETUP_SPEC,
            M3Disks.builder().
                disks(
                    "/mnt/mapr-storagefile1", 
                    "/mnt/mapr-storagefile2").
                commands(
                    "dd if=/dev/zero of=/mnt/mapr-storagefile1 bs=512M count=40",
                    "dd if=/dev/zero of=/mnt/mapr-storagefile2 bs=512M count=20").
                build() );
    }

    // can start in AWS by running this; or use brooklyn CLI
    public static void main(String[] args) {
        MyM3App app = new MyM3App();
        BrooklynLauncher.manage(app, 8081);
        app.start( new LocationRegistry().getLocationsById(["aws-ec2:us-east-1"]) );
    }
    
}
