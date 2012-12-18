package io.cloudsoft.mapr.m3;

import brooklyn.entity.Entity;
import com.google.common.collect.ImmutableList;
import io.cloudsoft.mapr.M3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class WorkerNode extends AbstractM3Node {

   public WorkerNode(Entity owner) {
      super(owner);
   }

   public WorkerNode(Map properties, Entity owner) {
      super(properties, owner);
   }

   public List<String> getAptPackagesToInstall() {
      return ImmutableList.<String>builder().add("mapr-fileserver", "mapr-tasktracker").addAll(super
              .getAptPackagesToInstall()).build();
   }

   public static final Logger log = LoggerFactory.getLogger(WorkerNode.class);

   public void startServices() {
      log.info("MapR node {} waiting for master", this);
      getConfig(M3.MASTER_UP);
      log.info("MapR node {} detected master up, proceeding to start warden", this);
      getDriver().startWarden();
   }

}
