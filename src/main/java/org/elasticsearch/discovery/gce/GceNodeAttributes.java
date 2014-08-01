package org.elasticsearch.discovery.gce;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNodeService.CustomAttributesProvider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

/**
 * Look up the availability zone of this GCE instance using the metadata
 * service and set it as a custom attribute of this ES node.
 */
public class GceNodeAttributes implements CustomAttributesProvider {

  private final Settings settings;
  private final ESLogger logger = Loggers.getLogger(getClass());
  
  public GceNodeAttributes(final Settings settings) {
    this.settings = settings;
  }
  
  @Override
  public Map<String, String> buildAttributes() {
    
    // only retrieve the zone if the setting is present
    if (settings.getAsBoolean("cloud.node.auto_attributes", true)) {
      Map<String,String> nodeAttributes = new HashMap<String,String>();
      
      URLConnection urlConnection;
      InputStream in = null;
      
      // call the metadata URL to pull back the zone that this node is in
      try {
          URL url = new URL("http://metadata/computeMetadata/v1beta1/instance/zone");
          this.logger.debug("Calling GCE metadata url {} to get the zone", url);

          urlConnection = url.openConnection();
          urlConnection.setConnectTimeout(2000);
          in = urlConnection.getInputStream();
          BufferedReader urlReader = new BufferedReader(new InputStreamReader(in));

          final String zoneResult = urlReader.readLine();
          if (zoneResult == null || zoneResult.length() == 0) {
              this.logger.error("No zone returned from {}", url);
              return null;
          }
          
          // set the zone result as an attribute
          nodeAttributes.put("gce_zone", zoneResult);
      } catch (IOException e) {
          this.logger.debug("Failed to get metadata: " + ExceptionsHelper.detailedMessage(e));
      } finally {
          IOUtils.closeWhileHandlingException(in);
      }
      
      // return the node attributes Map
      return nodeAttributes;
    } else {
      return null;
    }
  }

}
