package io.confluent.kafka.connect.cdc.xstream.docker;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.confluent.kafka.connect.cdc.docker.DockerExtension;
import io.confluent.kafka.connect.cdc.xstream.XStreamTestConstants;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class SettingsExtension implements ParameterResolver {
  @Override
  public boolean supports(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    boolean result = parameterContext.getParameter().isAnnotationPresent(Oracle12cSettings.class) ||
        parameterContext.getParameter().isAnnotationPresent(Oracle11gSettings.class);
    return result;
  }

  @Override
  public Object resolve(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    ExtensionContext.Namespace namespace = DockerExtension.namespace(extensionContext);
    ExtensionContext.Store store = extensionContext.getStore(namespace);
    DockerComposeRule dockerComposeRule = store.get(DockerExtension.STORE_SLOT_RULE, DockerComposeRule.class);
    Container container = dockerComposeRule.containers().container(XStreamTestConstants.ORACLE_CONTAINER);
    DockerPort dockerPort = container.port(XStreamTestConstants.ORACLE_PORT);
    return XStreamTestConstants.settings(dockerPort.getIp(), dockerPort.getExternalPort());
  }

}
