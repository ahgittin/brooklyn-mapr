package io.cloudsoft.mapr.m3;

import brooklyn.config.render.RendererHints;
import brooklyn.entity.Entity;
import brooklyn.event.adapter.FunctionSensorAdapter;
import brooklyn.event.basic.BasicAttributeSensor;
import brooklyn.location.Location;
import brooklyn.util.MutableMap;
import com.google.common.collect.ImmutableList;
import com.mysql.jdbc.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.ImmutableList.of;

public class MasterNode extends AbstractM3Node {

   public MasterNode(Entity owner) {
      super(owner);
   }

   public MasterNode(Map properties, Entity owner) {
      super(properties, owner);
   }

   public static final Logger log = LoggerFactory.getLogger(MasterNode.class);

   static String usedSpaceQuery =
           "select a.EVENT_TIME as \"timestamp\", a.NODE_ID as \"node\", " +
                   "a.M_VALUE as \"space\" from METRIC_TRANSACTION a" +
                   "inner join" +
                   "(select e.NODE_ID, e.M_NAME, max(e.EVENT_TIME) as max_time from METRIC_TRANSACTION e where e" +
                   ".M_NAME = \"SRVUSEDMB\" group by e.NODE_ID) b" +
                   "on a.EVENT_TIME = b.max_time and a.M_NAME = b.M_NAME group by a.NODE_ID order by a.NODE_ID;";

   static String availSpaceQuery =
           "select a.EVENT_TIME as \"timestamp\", a.NODE_ID as \"node\", " +
                   "a.M_VALUE as \"space\" from METRIC_TRANSACTION a" +
                   "inner join" +
                   "(select e.NODE_ID, e.M_NAME, max(e.EVENT_TIME) as max_time from METRIC_TRANSACTION e where e" +
                   ".M_NAME = \"SRVAVAILMB\" group by e.NODE_ID) b" +
                   "on a.EVENT_TIME = b.max_time and a.M_NAME = b.M_NAME group by a.NODE_ID order by a.NODE_ID;";

   public static final BasicAttributeSensor<Double> CLUSTER_USED_DFS_PERCENT =
           new BasicAttributeSensor<Double>(Double.class, "cluster.used.dfs.percent",
                   "The percentage o the cluster DFS that is currently being used.");


   public static final BasicAttributeSensor<String> MAPR_URL =
           new BasicAttributeSensor<String>(String.class, "mapr.url", "URL where MapR can be accessed");

   static {
      RendererHints.register(MAPR_URL, new RendererHints.NamedActionWithUrl("Open"));
   }

   private Connection connection;
   private PreparedStatement usedSpaceStatement;
   private PreparedStatement availableSpaceStatement;

   public boolean isZookeeper() { return true; }

   public List<String> getAptPackagesToInstall() {
      return ImmutableList.<String>builder().add("mapr-cldb", "mapr-jobtracker", "mapr-nfs", "mapr-webserver",
              "mapr-zookeeper").addAll(super.getAptPackagesToInstall()).build();
   }

   public void setupAdminUser() {
      getDriver().exec(of("sudo /opt/mapr/bin/maprcli acl edit -type cluster -user " + getUser() +
              ":fc"));
   }

   public void setupMySql() {
      log.info("Master node setting up mysql metrics storage...");
      getDriver().exec(of(
              // mysql needs this export in order not to require a password
              "export DEBIAN_FRONTEND=noninteractive;sudo -E -n -s -- apt-get install -y --allow-unauthenticated " +
                      "mysql-server",
              "sudo sed -i s/127.0.0.1/0.0.0.0/ /etc/mysql/my.cnf",
              "mysqladmin -u root password " + getPassword(),
              "sudo /etc/init.d/mysql restart",
              "echo \"GRANT ALL ON *.* TO '" + getUser() + "'@'%' IDENTIFIED BY '" + getPassword() + "';\" | sudo tee" +
                      " -a /tmp/grant-all-cmd.sql > /dev/null",
              "mysql -u root -p" + getPassword() + " < /tmp/grant-all-cmd.sql",
              "mysql -u root -p" + getPassword() + " < /opt/mapr/bin/setup.sql"));
      log.info("Mysql metrics storage setup.");
   }

   public void startMasterServices() {
      // start the services
      getDriver().exec(of("sudo /opt/mapr/bin/maprcli node services -nodes " + getAttribute
              (SUBNET_HOSTNAME) + " -nfs start"));
   }

   public void startServices() {
      getDriver().startWarden();
      setupAdminUser();
      startMasterServices();

      // not sure this sleep is necessary, but seems safer...
      try {
         Thread.sleep(10 * 1000);
      } catch (InterruptedException e) {
         propagate(e);
      }
      setAttribute(MAPR_URL, "https://" + getAttribute(HOSTNAME) + ":8443");

      try {
         Class.forName(Driver.class.getName()).newInstance();
         connection = DriverManager.getConnection("jdbc:mysql://" + getAttribute(HOSTNAME) + ":3306/metrics", getUser(),
                 getPassword());
         usedSpaceStatement = connection.prepareStatement(usedSpaceQuery);
         availableSpaceStatement = connection.prepareStatement(availSpaceQuery);
      } catch (Exception e) {
         propagate(e);
      }

      FunctionSensorAdapter dfsUsageSensor = sensorRegistry.register(new FunctionSensorAdapter(
              MutableMap.of("period", 1 * 5000),
              new Callable<Double>() {
                 public Double call() throws Exception {
                    // creating a new sql per query isnt the way to go

                    ResultSet usedSpaceResult = usedSpaceStatement.executeQuery();
                    ResultSet availableSpaceResult = availableSpaceStatement.executeQuery();

                    int sumUsed = 0;
                    while (usedSpaceResult.next()) {
                       sumUsed += usedSpaceResult.getInt("space");
                    }

                    int sumAvail = 0;
                    while (availableSpaceResult.next()) {
                       sumUsed += availableSpaceResult.getInt("space");
                    }

                    log.info("current dfs usage: " + 100 * sumUsed / (sumUsed + sumAvail));
                    return (100.0 * sumUsed) / (sumUsed + sumAvail);
                 }
              }));
      dfsUsageSensor.poll(CLUSTER_USED_DFS_PERCENT);


   }

   public void start(Collection<? extends Location> locations) {
      if (getPassword() == null)
         throw new IllegalArgumentException("configuration " + MAPR_PASSWORD.getName() + " must be specified");
      super.start(locations);
   }

   public boolean isMaster() { return true; }

   @Override
   public void stop() {
      try {
         connection.close();
      } catch (SQLException e) {
         propagate(e);
      }
      super.stop();
   }
}
