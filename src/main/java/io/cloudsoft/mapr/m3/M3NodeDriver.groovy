package io.cloudsoft.mapr.m3;

import io.cloudsoft.mapr.M3

import org.jclouds.compute.domain.ExecResponse
import org.jclouds.scriptbuilder.statements.java.InstallJDK
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import brooklyn.config.BrooklynLogging
import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver
import brooklyn.entity.basic.EntityLocal
import brooklyn.entity.basic.SoftwareProcessDriver
import brooklyn.entity.basic.lifecycle.CommonCommands
import brooklyn.entity.trait.Startable
import brooklyn.location.Location
import brooklyn.location.basic.SshMachineLocation
import brooklyn.location.basic.jclouds.JcloudsLocation.JcloudsSshMachineLocation
import brooklyn.util.MutableMap

import com.google.common.base.Throwables

public class M3NodeDriver extends AbstractSoftwareProcessSshDriver implements SoftwareProcessDriver {

    public static final Logger log = LoggerFactory.getLogger(M3NodeDriver.class);
    public static final Logger logSsh = LoggerFactory.getLogger(BrooklynLogging.SSH_IO);

    public static final String APT_GET_LINE = "deb http://package.mapr.com/releases/v1.2.7/ubuntu/ mapr optional";
    public static final String APT_GET_FILE = "/etc/apt/sources.list";
    
    public static final String DISKS_TXT_FQP = "/tmp/disks.txt";

    private boolean running = false;
    
    public M3NodeDriver(AbstractM3Node entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    public void repoUpdate() {
        exec(["""
if [ -f ${APT_GET_FILE} ] ; then 
    if [ -z \"`grep '${APT_GET_LINE}' ${APT_GET_FILE}`\" ] ; then 
        sudo sh -c 'echo ${APT_GET_LINE} >> ${APT_GET_FILE}'
    fi
    sudo apt-get update
elif [ -d /etc/yum.repos.d ] ; then
    sudo sh -c 'cat > /etc/yum.repos.d/maprtech.repo << __EOF__
[maprtech]
name=MapR Technologies
baseurl=http://package.mapr.com/releases/v1.2.7/redhat/
enabled=1
gpgcheck=0
protect=1

[maprecosystem]
name=MapR Technologies
baseurl=http://package.mapr.com/releases/ecosystem/redhat
enabled=1
gpgcheck=0
protect=1
__EOF__
'
else
    echo 'UNSUPPORTED OPERATING SYSTEM: yum or apt expected' 
    exit 44
fi
"""]);
    }
        
    public void repoInstall() {
        exec([
            CommonCommands.installPackage(null, 
                apt: "--allow-unauthenticated "+entity.getAptPackagesToInstall().join(" "),
                yum: entity.getAptPackagesToInstall().join(" "))
            ])
    }
    
    public void configureMapR() {
        String masterHostname = entity.getConfig(M3.MASTER_HOSTNAME);
        String zkHostnames = entity.getConfig(M3.ZOOKEEPER_HOSTNAMES).join(",");
        exec([ 
            // cldb (java) complains it needs at least 160k, on centos with openjdk7
            "if [ -f /etc/init.d/mapr-cldb ] ; then "+
                "sed -i s/XX:ThreadStackSize=128/XX:ThreadStackSize=256/ /etc/init.d/mapr-cldb ; fi",
            // now do the configuration
            "sudo /opt/mapr/server/configure.sh -C ${masterHostname} -Z ${zkHostnames}" ]);
    } 
    
    public void setupDisks() {
        //wait for machine to settle down... if it has trouble running script early
//        Thread.sleep(60*1000);
        
        DiskSetupSpec disks = entity.getConfig(AbstractM3Node.DISK_SETUP_SPEC);
        if (disks==null) throw new IllegalStateException("DISK_SETUP_SPEC is required for all nodes");
        
        log.info("${entity}: setting up disks");
        if (disks.getCommandsToRun()) {
            try {
                ((JcloudsSshMachineLocation)machine).execRemoteScript(disks.getCommandsToRun().toArray(new String[0]));
            } catch (Exception e) {
                log.warn("${entity}: error running custom commands for setting up disks; possibly a bug fixed in jclouds 1.4.1 (process will wait and script may run/finish normally): "+e);
                log.debug("${entity}: details of error running custom commands for setting up disks; possibly a bug fixed in jclouds 1.4.1: "+e, e);
                Thread.sleep(10*60*1000);
            }
        }
        machine.copyTo(new StringReader(disks.getPathsForDisksTxt().join("\n")), DISKS_TXT_FQP);
        exec([ "sudo /opt/mapr/server/disksetup -F ${DISKS_TXT_FQP}" ]);
        log.info("${entity}: disks set up, "+disks.getPathsForDisksTxt().join(" "));
    }

    public void startZookeeper() {
        Thread.sleep(10*1000);
        if (!entity.isZookeeper()) throw new IllegalStateException("${entity} is not a zookeeper node");
        log.info("${entity}: start zookeeper (${machine})");
        exec([ "sudo nohup /etc/init.d/mapr-zookeeper start" ]);
        entity.setAttribute(AbstractM3Node.ZOOKEEPER_UP, true);
    }
        
    public void startWarden() {
        log.info("${entity}: start warden");
        exec([ "sudo nohup /etc/init.d/mapr-warden start" ]);
        Thread.sleep(60*1000);
    }
    
    public void enableNonTtySudo() {
        newScript("disable requiretty").
            setFlag("allocatePTY", true).
            body.append(CommonCommands.dontRequireTtyForSudo()).
            execute();
    }
        
    public void installJdk7() {
        try {
            getLocation().acquireMutex("install:" + getLocation().getName(), "installing Java at " + getLocation());
            log.debug("checking for java at " + entity + " @ " + getLocation());
            int result = getLocation().execCommands("check java", Arrays.asList("java -version | grep 'java version' | grep 1.7.0"));
            if (result == 0) {
                log.debug("java detected at " + entity + " @ " + getLocation());
            } else {
                log.debug("java not detected at " + entity + " @ " + getLocation() + ", installing");
                log.info("${entity}: installing JDK");
                result = newScript("INSTALL_OPENJDK").body.append(
                            CommonCommands.installPackage(MutableMap.of("apt", "openjdk-7-jdk",
                                            "yum", "java-1.7.0-openjdk-devel"), null)
                            // TODO the following complains about yum-install not defined
                            // even though it is set as an alias (at the start of the first file)
//                            new ResourceUtils(this).getResourceAsString("classpath:///functions/setupPublicCurl.sh"),
//                            new ResourceUtils(this).getResourceAsString("classpath:///functions/installOpenJDK.sh"),
//                            "installOpenJDK"
                            ).execute();
                // hadoop doesn't find java 
                exec(["if [[ -d /etc/alternatives/java_sdk/ && ! -d /usr/java/default ]] ; then sudo mkdir -p /usr/java ; sudo ln -s /etc/alternatives/java_sdk /usr/java/default ; fi"])
                if (result!=0)
                        log.warn("Unable to install Java at " + getLocation() + " for " + entity +
                                " (and Java not detected); invalid result "+result+". " + 
                                "Processes may fail to start.");
                else
                    log.info("${entity}: installed JDK");
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            // as a fallback
            installJdkFromDlecan();
        
            getLocation().releaseMutex("install:" + getLocation().getName());
        }

        // //this works on ubuntu (surprising that jdk not in default repos!)
        // "sudo add-apt-repository ppa:dlecan/openjdk",
        // "sudo apt-get update",
        // "sudo apt-get install -y --allow-unauthenticated openjdk-7-jdk"
    }    
    
    public void installJdkFromDlecan() {
        int result = getLocation().execCommands("check java", Arrays.asList("java"));
        if (result==0) return;
        
        log.info("${entity}: failing back to legacy JDK install");
        //this seems to work best on ubuntu (jdk not in default repos for some images!)
        result = exec([
            "sudo add-apt-repository ppa:dlecan/openjdk < /dev/null",
			// command above fails in ubuntu 12, but isn't needed there
            "sudo apt-get update",
            "sudo apt-get install -y --allow-unauthenticated openjdk-7-jdk"
        ]);
        if (result==0)
            log.info("${entity}: installed JDK from legacy source");
        else {
            log.warn("${entity}: failed to install JDK from legacy source; trying JDK6");
            result = exec([
                "sudo apt-get install -y --allow-unauthenticated openjdk-6-jdk"
            ]);
            if (result==0)
                log.info("${entity}: installed JDK 6");
            else {
                log.warn("${entity}: failed to install JDK 6 from legacy source; failng");
                throw new IllegalStateException("Unable to install JDK 6 or 7 on ${entity}")
            }
        }
    }
    
    /** does common setup up to the point where zookeeper is running;
     * after this, node1 does some more things, then other nodes result when node1 is ready
     */
    public void runMaprPhase1() {
//        // was needed but don't think it is anymore
//        Thread.sleep(60*1000);
//        
        repoUpdate();
        repoInstall();
        configureMapR();
        setupDisks();
        if (entity.isZookeeper()) {
            startZookeeper();
        }
        else entity.setAttribute(AbstractM3Node.ZOOKEEPER_UP, false);
    }

    @Override
    public void start() {
        enableNonTtySudo()
        installJdk7();
        runMaprPhase1();
        //wait for ZK to be running everywhere
        entity.getConfig(M3.ZOOKEEPER_READY);
        entity.runMaprPhase2();
        running = true;
        entity.setAttribute(Startable.SERVICE_UP, true);
    }

    @Override public void install() { /* not used */ }
    @Override public void customize() { /* not used */ }
    @Override public void launch() { /* not used */ }
    
    @Override
    public void stop() {
        exec([ "sudo nohup /etc/init.d/mapr-warden stop" ]);
    }

    @Override
    public void kill() {
        stop();
    }

    @Override
    public boolean isRunning() {
        // TODO this is a poor man's test ... exec 'mapr-warden status' ?
        return running;
    }

    @Override
    public void restart() {
        exec([ "sudo nohup /etc/init.d/mapr-warden restart" ]);
    }
    
    public int exec(List<String> commands) {
        int result = machine.execCommands(logPrefix: ""+entity.getId()+"@"+machine.getName(), "M3:"+entity, commands);
        if (result!=0)
            log.warn("FAILED (exit status ${result}) running "+entity+" "+commands+"; subsequent commands may not work");
        return result;
    }

    @Override
    public void rebind() {
        // no-op
    }
}
