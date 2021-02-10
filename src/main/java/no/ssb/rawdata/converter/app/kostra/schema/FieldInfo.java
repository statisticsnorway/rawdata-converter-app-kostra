package no.ssb.rawdata.converter.app.kostra.schema;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class FieldInfo {
    private String name;
    private DataType dataType;
    private boolean optional;

    public static FieldInfo optionalString(String name) {
        return FieldInfo.builder()
          .name(name)
          .dataType(DataType.STRING)
          .optional(true)
          .build();
    }

    public static FieldInfo optionalBoolean(String name) {
        return FieldInfo.builder()
          .name(name)
          .dataType(DataType.BOOLEAN)
          .optional(true)
          .build();
    }

    public static FieldInfo optionalInt(String name) {
        return FieldInfo.builder()
          .name(name)
          .dataType(DataType.INT)
          .optional(true)
          .build();
    }

    public static FieldInfo optionalLong(String name) {
        return FieldInfo.builder()
          .name(name)
          .dataType(DataType.LONG)
          .optional(true)
          .build();
    }

    public static FieldInfo optionalDouble(String name) {
        return FieldInfo.builder()
          .name(name)
          .dataType(DataType.DOUBLE)
          .optional(true)
          .build();
    }

}
