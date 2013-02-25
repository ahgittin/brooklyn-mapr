package io.cloudsoft.mapr.m3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class M3Disks {

   public static DiskSetupSpecBuilder builder() {
      return new DiskSetupSpecBuilder();
   }

   public static class DiskSetupSpecBuilder implements DiskSetupSpec {
      private static final long serialVersionUID = 6764139005807297333L;

      final List<String> commandsToRun;
      final List<String> pathsForDisksTxt;

      public DiskSetupSpecBuilder() {
         this(Collections.<String>emptyList());
      }

      public DiskSetupSpecBuilder(List<String> pathsForDisksTxt) {
         this(Collections.<String>emptyList(), pathsForDisksTxt);
      }

      public DiskSetupSpecBuilder(List<String> commandsToRun, List<String> pathsForDisksTxt) {
         this.commandsToRun = Lists.newArrayList(commandsToRun);
         this.pathsForDisksTxt = Lists.newArrayList(pathsForDisksTxt);
      }

      @Override
      public List<String> getCommandsToRun() {
         return ImmutableList.copyOf(commandsToRun);
      }

      @Override
      public List<String> getPathsForDisksTxt() {
         return ImmutableList.copyOf(pathsForDisksTxt);
      }

      public DiskSetupSpecBuilder commands(String... commandsToRun) {
         return commands(Arrays.asList(commandsToRun));
      }

      public DiskSetupSpecBuilder commands(Collection<String> commandsToRun) {
         if (!this.commandsToRun.isEmpty() && !commandsToRun.isEmpty()) throw new IllegalStateException(
                 "cannot set commands on builder when it already has commands set; clear the list first");
         this.commandsToRun.clear();
         this.commandsToRun.addAll(commandsToRun);
         return this;
      }

      public DiskSetupSpecBuilder disks(String... pathsForDisksTxt) {
         return disks(Arrays.asList(pathsForDisksTxt));
      }

      public DiskSetupSpecBuilder disks(Collection<String> pathsForDisksTxt) {
         if (!this.pathsForDisksTxt.isEmpty() && !pathsForDisksTxt.isEmpty()) throw new IllegalStateException(
                 "cannot set disks on builder when it already has disks set; clear the list first");
         this.pathsForDisksTxt.clear();
         this.pathsForDisksTxt.addAll(pathsForDisksTxt);
         return this;
      }

      public DiskSetupSpec build() {
         return new ImmutableDiskSetupSpec(commandsToRun, pathsForDisksTxt);
      }
   }

}
