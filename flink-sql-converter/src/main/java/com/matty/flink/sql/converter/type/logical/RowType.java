package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.codehaus.commons.nullanalysis.Nullable;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.table.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class RowType extends LogicalType {

    private static final String FORMAT = "ROW<%s>";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            Row.class.getName(),
            "org.apache.flink.table.dataformat.BaseRow");

    private static final Class<?> DEFAULT_CONVERSION = Row.class;

    /**
     * Describes a field of a {@link RowType}.
     */
    public static final class RowField implements Serializable {

        private static final String FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";

        private static final String FIELD_FORMAT_NO_DESCRIPTION = "%s %s";

        private final String name;

        private final LogicalType type;

        private final @Nullable
        String description;

        public RowField(String name, LogicalType type, @Nullable String description) {
            this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
            this.type = Preconditions.checkNotNull(type, "Field type must not be null.");
            this.description = description;
        }

        public RowField(String name, LogicalType type) {
            this(name, type, null);
        }

        public String getName() {
            return name;
        }

        public LogicalType getType() {
            return type;
        }

        public Optional<String> getDescription() {
            return Optional.ofNullable(description);
        }

        public RowField copy() {
            return new RowField(name, type.copy(), description);
        }

        public String asSummaryString() {
            return formatString(type.asSummaryString(), true);
        }

        public String asSerializableString() {
            return formatString(type.asSerializableString(), false);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RowField rowField = (RowField) o;
            return name.equals(rowField.name) &&
                    type.equals(rowField.type) &&
                    Objects.equals(description, rowField.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, description);
        }

        private String formatString(String typeString, boolean excludeDescription) {
            if (description == null) {
                return String.format(FIELD_FORMAT_NO_DESCRIPTION,
                        escapeIdentifier(name),
                        typeString);
            } else if (excludeDescription) {
                return String.format(FIELD_FORMAT_WITH_DESCRIPTION,
                        escapeIdentifier(name),
                        typeString,
                        "...");
            } else {
                return String.format(FIELD_FORMAT_WITH_DESCRIPTION,
                        escapeIdentifier(name),
                        typeString,
                        escapeSingleQuotes(description));
            }
        }
    }

    private final List<RowField> fields;

    public RowType(boolean isNullable, List<RowField> fields) {
        super(isNullable, LogicalTypeRoot.ROW);
        this.fields = Collections.unmodifiableList(
                new ArrayList<>(
                        Preconditions.checkNotNull(fields, "Fields must not be null.")));

        validateFields(fields);
    }

    public RowType(List<RowField> fields) {
        this(true, fields);
    }

    public List<RowField> getFields() {
        return fields;
    }

    public List<String> getFieldNames() {
        return fields.stream().map(RowField::getName).collect(Collectors.toList());
    }

    public LogicalType getTypeAt(int i) {
        return fields.get(i).getType();
    }

    public int getFieldCount() {
        return fields.size();
    }

    public int getFieldIndex(String fieldName) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new RowType(
                isNullable,
                fields.stream().map(RowField::copy).collect(Collectors.toList()));
    }

    @Override
    public String asSummaryString() {
        return withNullability(
                FORMAT,
                fields.stream()
                        .map(RowField::asSummaryString)
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public String asSerializableString() {
        return withNullability(
                FORMAT,
                fields.stream()
                        .map(RowField::asSerializableString)
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.unmodifiableList(
                fields.stream()
                        .map(RowField::getType)
                        .collect(Collectors.toList()));
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        return fields.equals(rowType.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    // --------------------------------------------------------------------------------------------

    private static void validateFields(List<RowField> fields) {
        final List<String> fieldNames = fields.stream()
                .map(f -> f.name)
                .collect(Collectors.toList());
        if (fieldNames.stream().anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
            throw new ValidationException("Field names must contain at least one non-whitespace character.");
        }
        final Set<String> duplicates = fieldNames.stream()
                .filter(n -> Collections.frequency(fieldNames, n) > 1)
                .collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new ValidationException(
                    String.format("Field names must be unique. Found duplicates: %s", duplicates));
        }
    }

    public static RowType of(LogicalType... types) {
        List<RowField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new RowField("f" + i, types[i]));
        }
        return new RowType(fields);
    }

    public static RowType of(LogicalType[] types, String[] names) {
        List<RowField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new RowField(names[i], types[i]));
        }
        return new RowType(fields);
    }
}

