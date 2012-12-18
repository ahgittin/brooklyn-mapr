package io.cloudsoft.mapr.m3;

import brooklyn.config.BrooklynLogging;
import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.SoftwareProcessDriver;
import brooklyn.entity.basic.lifecycle.CommonCommands;
import brooklyn.entity.trait.Startable;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.location.basic.jclouds.JcloudsLocation.JcloudsSshMachineLocation;
import brooklyn.util.MutableMap;
import com.google.common.collect.ImmutableMap;
import io.cloudsoft.mapr.M3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Joiner.on;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.ImmutableList.of;

public class M3NodeDriver extends AbstractSoftwareProcessSshDriver implements SoftwareProcessDriver {

    public static final Logger log = LoggerFactory.getLogger(M3NodeDriver.class);
    public static final Logger logSsh = LoggerFactory.getLogger(BrooklynLogging.SSH_IO);

    public static final String APT_GET_LINE = "deb http://package.mapr.com/releases/v2.1.1/ubuntu/ mapr optional";
    public static final String APT_GET_FILE = "/etc/apt/sources.list";
    
    public static final String DISKS_TXT_FQP = "/tmp/disks.txt";

    private boolean running = false;
   private AbstractM3Node entity;

   public M3NodeDriver(AbstractM3Node entity, SshMachineLocation machine) {
      super(entity, machine);
      this.entity = entity;
   }

   public void repoUpdate() {
      exec(of(
              "if [ -f " + APT_GET_FILE + " ] ; then\n" +
                      "    if [ -z \"`grep '" + APT_GET_LINE + "' " + APT_GET_FILE + "`\" ] ; then\n" +
                      "        sudo sh -c 'echo " + APT_GET_LINE + " >> " + APT_GET_FILE + "'\n" +
                      "    fi\n" +
                      "    sudo apt-get update\n" +
                      "elif [ -d /etc/yum.repos.d ] ; then" +
                      "    sudo sh -c 'cat > /etc/yum.repos.d/maprtech.repo << __EOF__\n" +
                      "[maprtech]\n" +
                      "name=MapR Technologies\n" +
                      "baseurl=http://package.mapr.com/releases/v1.2.7/redhat/\n" +
                      "enabled=1\n" +
                      "gpgcheck=0\n" +
                      "protect=1\n" +
                      "\n" +
                      "[maprecosystem]" +
                      "name=MapR Technologies" +
                      "baseurl=http://package.mapr.com/releases/ecosystem/redhat\n" +
                      "enabled=1\n" +
                      "gpgcheck=0\n" +
                      "protect=1\n" +
                      "__EOF__\n" +
                      "'\n" +
                      "else\n" +
                      "    echo 'UNSUPPORTED OPERATING SYSTEM: yum or apt expected'\n" +
                      "    exit 44\n" +
                      "fi\n" +
                      "\n"));
   }

   public void repoInstall() {
      exec(of(
              CommonCommands.installPackage(ImmutableMap.of(
                      "apt", "--allow-unauthenticated " + on(" ").join(entity.getAptPackagesToInstall()),
                      "yum", on(" ").join(entity.getAptPackagesToInstall())), null
              )));
   }

   public void configureMapR() {
      String masterHostname = entity.getConfig(M3.MASTER_HOSTNAME);
      String zkHostnames = on(",").join(entity.getConfig(M3.ZOOKEEPER_HOSTNAMES));
      exec(of(
              // cldb (java) complains it needs at least 160k, on centos with openjdk7
              "if [ -f /etc/init.d/mapr-cldb ] ; then \n" +
                      "sudo sed -i s/XX:ThreadStackSize=128/XX:ThreadStackSize=256/ /etc/init.d/mapr-cldb ;fi",
              // now do the configuration
              "sudo /opt/mapr/server/configure.sh -C " + masterHostname + " -Z " + zkHostnames + ""));
   }

   public void setupDisks() {
      //wait for machine to settle down... if it has trouble running script early
//        Thread.sleep(60*1000);
        
        DiskSetupSpec disks = entity.getConfig(AbstractM3Node.DISK_SETUP_SPEC);
        if (disks==null) throw new IllegalStateException("DISK_SETUP_SPEC is required for all nodes");

      log.info(getEntity() + ": setting up disks");
      if (disks.getCommandsToRun() != null) {
         try {
            ((JcloudsSshMachineLocation) getMachine()).execRemoteScript(disks.getCommandsToRun().toArray(new
                    String[0]));
         } catch (Exception e) {
            log.warn(getEntity() + ": error running custom commands for setting up disks; possibly a bug fixed in " +
                    "jclouds 1.4.1 (process will wait and script may run/finish normally): " + e);
            log.debug(getEntity() + ": details of error running custom commands for setting up disks; possibly a bug " +
                    "fixed in jclouds 1.4.1: " + e, e);

            try {
               Thread.sleep(10 * 60 * 1000);
            } catch (InterruptedException e1) {
               propagate(e);
            }
         }
      }
      getMachine().copyTo(new StringReader(on("\n").join(disks.getPathsForDisksTxt())), DISKS_TXT_FQP);
      exec(of("sudo /opt/mapr/server/disksetup -F " + DISKS_TXT_FQP));
      log.info(getEntity() + ": disks set up, " + on(" ").join(disks.getPathsForDisksTxt()));
   }

   public void startZookeeper() {
      try {
         Thread.sleep(10 * 1000);
      } catch (InterruptedException e) {
         propagate(e);
      }
      if (!entity.isZookeeper()) throw new IllegalStateException(getEntity() + " is not a zookeeper node");
      log.info(getEntity() + ": start zookeeper (" + getMachine() + ")");
      exec(of("sudo nohup /etc/init.d/mapr-zookeeper start"));
      entity.setAttribute(AbstractM3Node.ZOOKEEPER_UP, true);
   }

   public void startWarden() {
      log.info(getEntity() + ": start warden");
      exec(of("sudo nohup /etc/init.d/mapr-warden start"));
      try {
         Thread.sleep(60 * 1000);
      } catch (InterruptedException e) {
         propagate(e);
      }
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
         int result = getLocation().execCommands("check java", Arrays.asList("java -version | grep 'java version' " +
                 "| grep 1.7.0"));
         if (result == 0) {
            log.debug("java detected at " + entity + " @ " + getLocation());
         } else {
            log.debug("java not detected at " + entity + " @ " + getLocation() + ", installing");
            log.info(getEntity() + ": installing JDK");
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
            exec(of("if [[ -d /etc/alternatives/java_sdk/ && ! -d /usr/java/default ]] ; then sudo mkdir -p " +
                    "/usr/java ; sudo ln -s /etc/alternatives/java_sdk /usr/java/default ; fi"));
            if (result != 0)
               log.warn("Unable to install Java at " + getLocation() + " for " + entity +
                       " (and Java not detected); invalid result " + result + ". " +
                       "Processes may fail to start.");
            else
               log.info(getEntity() + ": installed JDK");
         }
      } catch (Exception e) {
         propagate(e);
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

       log.info(getEntity() + ": failing back to legacy JDK7 install");
       //this seems to work best on ubuntu (jdk not in default repos for some images!)
       result = exec(of(
               "sudo add-apt-repository ppa:dlecan/openjdk < /dev/null",
               // command above fails in ubuntu 12, but isn't needed there
               "sudo apt-get update",
               "sudo apt-get install -y --allow-unauthenticated openjdk-7-jdk",
               // need to set java home for mapr
               "echo \"JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64\" | sudo tee -a /etc/environment > /dev/null"
       ));
       if (result == 0)
          log.info(getEntity() + ": installed JDK7 from legacy source");
       else {
          log.warn(getEntity() + ": failed to install JDK7 from legacy source; trying JDK6");
          result = exec(of("sudo apt-get install -y --allow-unauthenticated openjdk-6-jdk"));
          if (result == 0)
             log.info(getEntity() + ": installed JDK 6");
          else {
             log.warn(getEntity() + ": failed to install JDK 6 from legacy source; failng");
             throw new IllegalStateException("Unable to install JDK 6 or 7 on " + getEntity());
          }
       }
    }
    
    /** does common setup up to the point where zookeeper is running;
     * after this, node1 does some more things, then other nodes result when node1 is ready
     */
    public void configure() {
//        // was needed but don't think it is anymore
//        Thread.sleep(60*1000);
//
       AbstractM3Node entity = (AbstractM3Node) getEntity();

       log.info("Setting up MapR user and configuring package repositories on: " + entity);
       entity.setupMapRUser();
       repoUpdate();
       repoInstall();
       log.info("Configuring MapR on: " + entity);
       configureMapR();
       setupDisks();
       if (entity.isMaster()) {
          log.info("Setting up mysql on master: " + entity);
          ((MasterNode) entity).setupMySql();
       }
       if (entity.isZookeeper()) {
          log.info("Starting zookeeper on: " + entity);
          startZookeeper();
       } else entity.setAttribute(AbstractM3Node.ZOOKEEPER_UP, false);

       log.info("Configure phase done on: " + entity);
    }

    @Override
    public void start() {
       AbstractM3Node entity = (AbstractM3Node) getEntity();

       entity.setAttribute(AbstractM3Node.SUBNET_HOSTNAME, entity.getLocalHostname());
       enableNonTtySudo();
       log.info("Installing JDK on:" + getEntity());
       installJdk7();
       log.info("Installing/Configuring packages and starting required services on: " + getEntity());
       configure();
       //wait for ZK to be running everywhere
       entity.getConfig(M3.ZOOKEEPER_READY);
       entity.configureMetrics(entity.getConfig(M3.MASTER_HOSTNAME));
       log.info("Starting MapR services on: " + getEntity());
       entity.startServices();
       running = true;
       entity.setAttribute(Startable.SERVICE_UP, true);
    }

    @Override public void install() { /* not used */ }
    @Override public void customize() { /* not used */ }
    @Override public void launch() { /* not used */ }
    
    @Override
    public void stop() {
       exec(of("sudo nohup /etc/init.d/mapr-warden stop"));
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
       exec(of("sudo nohup /etc/init.d/mapr-warden restart"));
    }
    
    public int exec(List<String> commands) {
       int result = getMachine().execCommands(ImmutableMap.of("logPrefix", entity.getId() + "@" + getMachine()
               .getName()), "M3:" + entity, commands);
       if (result != 0)
          log.warn("FAILED (exit status ${result}) running " + entity + " " + commands + "; subsequent commands may " +
                  "not work");
       return result;
    }

    @Override
    public void rebind() {
        // no-op
    }
}
