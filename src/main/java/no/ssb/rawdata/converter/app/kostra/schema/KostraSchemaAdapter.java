package no.ssb.rawdata.converter.app.kostra.schema;

import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.util.RawdataMessageAdapter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KostraSchemaAdapter {

    private static final String FIELDNAME_STRUCTURE = "structure";
    private static final String RECORDNAME_STRUCTURE_ITEM = "structureItem";
    private static final String FIELDNAME_DATA = "data";
    private static final String RECORDNAME_DATA_ITEM = "dataItem";

    final static Schema STRUCTURE_ITEM_SCHEMA = itemSchemaOf(RECORDNAME_STRUCTURE_ITEM, List.of(
      FieldInfo.optionalString("name"),
      FieldInfo.optionalString("type"),
      FieldInfo.optionalString("role")
    ));

    final static Schema STRUCTURE_SCHEMA = SchemaBuilder.map().values(STRUCTURE_ITEM_SCHEMA);

    private final Schema dataItemSchema;

    public KostraSchemaAdapter(Schema dataItemSchema) {
        this.dataItemSchema = dataItemSchema;
    }

    public List<String> getFieldNames() {
        return dataItemSchema.getFields().stream()
          .map(f -> f.name())
          .collect(Collectors.toList());
    }

    public Schema getTargetSchema() {
        return SchemaBuilder.record("record").fields()
          .name(FIELDNAME_STRUCTURE).type(STRUCTURE_SCHEMA).noDefault()
          .name(FIELDNAME_DATA).type(getDataSchema()).noDefault()
          .endRecord();
    }

    public Schema getDataSchema() {
        return dataItemSchema;
    }

    public GenericRecord targetRecordOf(KostraDataEnvelope dataEnvelope) {
        GenericRecordBuilder record = new GenericRecordBuilder(getTargetSchema());

        // structure
        Map<String, GenericRecord> structureAsMap = dataEnvelope.getStructure().stream()
          .collect(Collectors.toMap(
            KostraDataEnvelope.StructureItem::getName,
            i -> new GenericRecordBuilder(STRUCTURE_ITEM_SCHEMA)
              .set("name", i.getName())
              .set("type", i.getType())
              .set("role", i.getRole())
              .build()
            )
          );
        record.set(FIELDNAME_STRUCTURE, structureAsMap);

        // data
        GenericRecordBuilder dataRecord = new GenericRecordBuilder(dataItemSchema);
        List<String> fieldNames = getFieldNames();
        List<Object> dataItem = dataEnvelope.getDataItem();
        for (int i=0; i<fieldNames.size(); i++) {
            dataRecord.set(fieldNames.get(i), dataItem.get(i));
        }
        record.set(FIELDNAME_DATA, dataRecord.build());

        return record.build();
    }

    public static KostraSchemaAdapter of(RawdataMessage rawdataMessage, String rawdataItemName) {
        // Get hold of data collector schema metadata
        Map<String, Object> schema = new RawdataMessageAdapter(rawdataMessage)
          .findItemMetadata(rawdataItemName)
          .orElseThrow(() -> new KostraSchemaException("No item metadata found for '" + rawdataItemName + "' sample item. Unable to determine target avro schema.")
          ).getSchemaMap();

        // Transform schema to field info
        List<FieldInfo> dataFields = ((List<Map<String,Object>>) schema.getOrDefault("fields", List.of()))
          .stream().map(f ->
            FieldInfo.builder()
              .name((String) f.get("name"))
              .dataType(DataType.from((String) f.get("data-type")))
              .optional(true)
              .build()
          )
          .collect(Collectors.toList());

        if (dataFields.isEmpty()) {
            new KostraSchemaException("No fields schema metadata found in sample item. Unable to determine target avro schema.");
        }

        Schema dataItemSchema = itemSchemaOf(RECORDNAME_DATA_ITEM, dataFields);

        return new KostraSchemaAdapter(dataItemSchema);
    }

    private static Schema itemSchemaOf(String recordName, List<FieldInfo> fields) {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(recordName).fields();
        for (FieldInfo fieldInfo : fields) {
            String name = fieldInfo.getName();
            switch (fieldInfo.getDataType()) {
                case INT: fieldAssembler.optionalInt(name); break;
                case LONG: fieldAssembler.optionalLong(name); break;
                case BOOLEAN: fieldAssembler.optionalBoolean(name); break;
                default: fieldAssembler.optionalString(name);
            }
        }

        return fieldAssembler.endRecord();
    }

    public static class KostraSchemaException extends RawdataConverterException {
        public KostraSchemaException(String msg) {
            super(msg);
        }
    }

}