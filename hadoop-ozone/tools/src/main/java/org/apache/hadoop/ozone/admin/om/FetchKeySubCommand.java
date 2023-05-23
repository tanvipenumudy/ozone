
package org.apache.hadoop.ozone.admin.om;

import java.util.Base64;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import javax.crypto.SecretKey;

/**
 * Handler of ozone admin om fetch-current-key command.
 */
@CommandLine.Command(
    name = "fetch-current-key",
    description = "CLI command to fetch the latest key",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class FetchKeySubCommand implements Callable<Void> {
  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID",
      required = true
  )
  private String omServiceId;

  @Override
  public Void call() throws Exception {
    OzoneManagerProtocol client = parent.createOmClient(omServiceId);
    ManagedSecretKey managedSecretKey = client.getCurrentSecretKey();
    SecretKey key = managedSecretKey.getSecretKey();
    byte[] encodedKey = key.getEncoded();
    String keyString = Base64.getEncoder().encodeToString(encodedKey);
    System.out.println("Current Secret Key: " + keyString);
    return null;
  }
}

