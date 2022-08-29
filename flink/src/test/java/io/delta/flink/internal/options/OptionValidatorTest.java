package io.delta.flink.internal.options;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.flink.configuration.ConfigOptions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OptionValidatorTest {
    @Test
    public void testValidate_missingOption() throws Exception {
        Map<String, DeltaConfigOption<?>> validOptions = new HashMap<>();
        DeltaConnectorConfiguration config = new DeltaConnectorConfiguration();
        OptionValidator validator = new OptionValidator(config, validOptions);

        assertThrows(IllegalArgumentException.class, () -> {
            DeltaConfigOption<String> str = validator.validateOptionName("missing_option");
        });
    }

    @Test
    public void testValidate_validOption() throws Exception {
        Map<String, DeltaConfigOption<?>> validOptions = new HashMap<>();
        DeltaConnectorConfiguration config = new DeltaConnectorConfiguration();

        validOptions.put(
            "valid_option",
            DeltaConfigOption.of(
                ConfigOptions
                    .key("valid_option")
                    .longType()
                    .defaultValue(100L)
                    .withDescription("timeout"),
                Long.class));

        OptionValidator validator = new OptionValidator(config, validOptions);
        DeltaConfigOption<Long> opt = validator.validateOptionName("valid_option");
        assertEquals("valid_option", opt.key());
        assertEquals(100L, opt.defaultValue());
    }

    @Test
    public void testSetOption_validOption() throws Exception {
        Map<String, DeltaConfigOption<?>> validOptions = new HashMap<>();
        DeltaConnectorConfiguration config = new DeltaConnectorConfiguration();

        DeltaConfigOption<?> intOption =
            DeltaConfigOption.of(
                ConfigOptions.key("int").intType().defaultValue(10),
                Integer.class);
        validOptions.put("int", intOption);

        DeltaConfigOption<?> stringOption =
            DeltaConfigOption.of(
                ConfigOptions.key("string").stringType().defaultValue(""),
                String.class
            );
        validOptions.put("string", stringOption);

        DeltaConfigOption<?> longOption =
            DeltaConfigOption.of(
                ConfigOptions.key("long").longType().defaultValue(10L),
                Long.class
            );
        validOptions.put("long", longOption);

        DeltaConfigOption<?> boolOption =
            DeltaConfigOption.of(
                ConfigOptions.key("bool").booleanType().defaultValue(false),
                Boolean.class
            );
        validOptions.put("bool", boolOption);

        OptionValidator validator = new OptionValidator(config, validOptions);

        validator.option("string", "string");
        validator.option("int", 20);
        validator.option("long", 100L);
        validator.option("bool", true);

        assertEquals(
            new HashSet<>(Arrays.asList("string", "int", "long", "bool")),
            config.getUsedOptions());
        assertEquals("string", config.getValue(stringOption));
        assertEquals(20, config.getValue(intOption));
        assertEquals(100L, config.getValue(longOption));
        assertEquals(true, config.getValue(boolOption));
    }

    @Test
    public void testSetOption_missingOption() throws Exception {
        Map<String, DeltaConfigOption<?>> validOptions = new HashMap<>();
        DeltaConnectorConfiguration config = new DeltaConnectorConfiguration();
        OptionValidator validator = new OptionValidator(config, validOptions);

        DeltaConfigOption<?> boolOption =
            DeltaConfigOption.of(
                ConfigOptions.key("bool").booleanType().defaultValue(false),
                Boolean.class
            );
        validOptions.put("bool", boolOption);

        assertThrows(IllegalArgumentException.class, () -> {
            validator.option("string", "'");
        });

        assertEquals(new HashSet<>(), config.getUsedOptions());
    }
}
