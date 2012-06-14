package io.cloudsoft.mapr.m3;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.ImmutableList;

public interface DiskSetupSpec extends Serializable {

    public List<String> getCommandsToRun();
    public List<String> getPathsForDisksTxt();

    public static class ImmutableDiskSetupSpec implements DiskSetupSpec {
        private static final long serialVersionUID = 6764139005807297333L;
        
        final List<String> commandsToRun;
        final List<String> pathsForDisksTxt;
        
        public ImmutableDiskSetupSpec(List<String> commandsToRun, List<String> pathsForDisksTxt) {
            this.commandsToRun = ImmutableList.copyOf(commandsToRun);
            this.pathsForDisksTxt = ImmutableList.copyOf(pathsForDisksTxt);
        }
        
        @Override
        public List<String> getCommandsToRun() {
            return commandsToRun;
        }

        @Override
        public List<String> getPathsForDisksTxt() {
            return pathsForDisksTxt;
        }
    }
    
}
