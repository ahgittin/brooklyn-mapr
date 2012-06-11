package io.cloudsoft.mapr

import io.cloudsoft.mapr.m3.AbstractM3Node
import io.cloudsoft.mapr.m3.DiskSetupSpec.BasicDiskSetupSpec
import brooklyn.entity.basic.AbstractApplication
import brooklyn.launcher.BrooklynLauncher;
import brooklyn.location.basic.LocationRegistry;

/** starts an M3 cluster in AWS. login as username 'ubuntu', password 'm4pr'. */
public class MyM3App extends AbstractApplication {

    M3 m3 = new M3(this);
    
    { 
        m3.setConfig(AbstractM3Node.DISK_SETUP_SPEC,
            new BasicDiskSetupSpec(
                ["echo hello going to dd", "ls /", 
                    "dd if=/dev/zero of=/mnt/mapr-storagefile bs=512M count=40",
                    "echo done dd",
                    "ls -al /mnt"],
                ["/mnt/mapr-storagefile"]
            ) );
    }

    public static void main(String[] args) {
        MyM3App app = new MyM3App();
        BrooklynLauncher.manage(app);
        app.start( new LocationRegistry().getLocationsById(["aws-ec2:us-east-1"]) );
    }
    
}
