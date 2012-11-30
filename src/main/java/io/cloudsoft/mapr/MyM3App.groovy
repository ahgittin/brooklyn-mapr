package io.cloudsoft.mapr

import io.cloudsoft.mapr.m3.AbstractM3Node
import io.cloudsoft.mapr.m3.M3Disks
import io.cloudsoft.mapr.m3.MasterNode
import brooklyn.entity.basic.AbstractApplication
import brooklyn.launcher.BrooklynLauncher
import brooklyn.launcher.BrooklynServerDetails
import brooklyn.location.Location
import brooklyn.util.CommandLineUtil

/** starts an M3 cluster in AWS. login as username 'ubuntu', password 'm4pr'. */
public class MyM3App extends AbstractApplication {

    final static String DEFAULT_LOCATION =
        "aws-ec2:us-east-1"; 
//        "cloudstack-citrix";
//        "aws-ec2-us-east-1-centos";
        
    M3 m3 = new M3(this);
    
    {
        // EDIT to choose your own password
        // (you can also specify a MAPR_USERNAME; the default is 'mapr')
        setConfig(MasterNode.MAPR_PASSWORD, "m4pr");
         
        // disks can be set specifically on any entity; 
        // setting on the root will provide a default which is inherited everywhere 
        // this default is an on-disk flat-file
        setConfig(AbstractM3Node.DISK_SETUP_SPEC,
            M3Disks.builder().
                disks(
                    "/mnt/mapr-storagefile1", 
                    "/mnt/mapr-storagefile2").
                commands(
                    "sudo truncate -s 20G /mnt/mapr-storagefile1",
                    "sudo truncate -s 10G /mnt/mapr-storagefile2").
                build() );
    }

    // can start in AWS by running this -- or use brooklyn CLI/REST for most clouds, or programmatic/config for set of fixed IP machines
    public static void main(String[] argv) {
        MyM3App app = new MyM3App();
        
        List args = new ArrayList(Arrays.asList(argv));
        BrooklynServerDetails server = BrooklynLauncher.newLauncher().
                webconsolePort( CommandLineUtil.getCommandLineOption(args, "--port", "8081+") ).
                managing(app).
                launch();

        List<Location> locations = server.getManagementContext().getLocationRegistry().resolve(args ?: [DEFAULT_LOCATION])
        app.start(locations)
    }
    
}
