package io.kafbat.ui.serde.sbe;

import io.kafbat.ui.serde.api.PropertyResolver;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link PropertyResolver} over a plain map; like kafka-ui, a comma-separated string binds to a list.
 */
record MapPropertyResolver(Map<String, Object> properties) implements PropertyResolver {

  @Override
  public <T> Optional<T> getProperty(final String key, final Class<T> targetType) {
    return Optional.ofNullable(properties.get(key)).map(targetType::cast);
  }

  @Override
  public <T> Optional<List<T>> getListProperty(final String key, final Class<T> itemType) {
    return Optional.ofNullable(properties.get(key)).map(value -> {
      final List<?> items = value instanceof List<?> list ? list : Arrays.asList(value.toString().split(",", -1));
      return items.stream().map(itemType::cast).toList();
    });
  }

  @Override
  public <K, V> Optional<Map<K, V>> getMapProperty(final String key, final Class<K> keyType, final Class<V> valueType) {
    return Optional.empty();
  }
}
