package org.apache.hadoop.ozone.admin.scm;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

/**
 * Handler of ozone admin scm rotate command.
 */
@CommandLine.Command(
    name = "rotate",
    description = "CLI command to force generate new keys (rotate)",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class RotateKeySubCommand extends ScmSubcommand {

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @Override
  protected void execute(ScmClient scmClient) throws IOException {
    try (ScmClient client = new ContainerOperationClient(
        parent.getParent().getOzoneConf())) {
      boolean status = false;
      try {
        status = client.checkAndRotate(true);
      } catch (TimeoutException e) {
        e.printStackTrace();
      }
      System.out.println(
          "Secret key rotation is complete, a new key has been generated. " +
              "Rotate Status: " + status);
    }
  }
}
