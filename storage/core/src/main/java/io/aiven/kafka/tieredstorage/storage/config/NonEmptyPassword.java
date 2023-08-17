package io.aiven.kafka.tieredstorage.storage.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Objects;

public class NonEmptyPassword implements ConfigDef.Validator {
    @Override
    public void ensureValid(final String name, final Object value) {
        if (Objects.isNull(value)) {
            return;
        }
        final var pwd = (Password) value;
        if (pwd.value() == null || pwd.value().isBlank()) {
            throw new ConfigException(name + " value must not be empty");
        }
    }
}
