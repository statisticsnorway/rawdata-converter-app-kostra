package no.ssb.rawdata.converter.app.kostra;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dlp.pseudo.core.util.Json;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.app.kostra.schema.KostraDataEnvelope;
import no.ssb.rawdata.converter.app.kostra.schema.KostraSchemaAdapter;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ConversionResult.ConversionResultBuilder;
import no.ssb.rawdata.converter.core.convert.RawdataConverter;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.schema.AggregateSchemaBuilder;
import no.ssb.rawdata.converter.core.schema.DcManifestSchemaAdapter;
import no.ssb.rawdata.converter.metrics.MetricName;
import no.ssb.rawdata.converter.util.RawdataMessageAdapter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.Collection;

@Slf4j
@RequiredArgsConstructor
public class KostraRawdataConverter implements RawdataConverter {

    private static final String RAWDATA_ITEMNAME_ENTRY = "entry";
    private static final String FIELDNAME_MANIFEST = "manifest";
    private static final String FIELDNAME_COLLECTOR = "collector";
    private static final String FIELDNAME_RECORD = "record";

    @NonNull private final KostraRawdataConverterConfig converterConfig;
    @NonNull private final ValueInterceptorChain valueInterceptorChain;

    private DcManifestSchemaAdapter dcManifestSchemaAdapter;
    private KostraSchemaAdapter kostraSchemaAdapter;
    private Schema manifestSchema;
    private Schema targetAvroSchema;

    @Override
    public void init(Collection<RawdataMessage> sampleRawdataMessages) {
        log.info("Determine target avro schema from {}", sampleRawdataMessages);
        RawdataMessage sample = sampleRawdataMessages.stream()
          .findFirst()
          .orElseThrow(() ->
            new KostraRawdataConverterException("Unable to determine target avro schema since no sample rawdata messages were supplied. Make sure to configure `converter-settings.rawdata-samples`")
          );

        RawdataMessageAdapter msg = new RawdataMessageAdapter(sample);
        dcManifestSchemaAdapter = DcManifestSchemaAdapter.of(sample);
        kostraSchemaAdapter = KostraSchemaAdapter.of(sample, RAWDATA_ITEMNAME_ENTRY);
        log.info("Data column names: {}", kostraSchemaAdapter.getFieldNames());

        String targetNamespace = "dapla.rawdata.kostra." + msg.getTopic().orElse("dataset");

        manifestSchema = new AggregateSchemaBuilder("dapla.rawdata.manifest")
          .schema(FIELDNAME_COLLECTOR, dcManifestSchemaAdapter.getDcManifestSchema())
          .build();

        targetAvroSchema = new AggregateSchemaBuilder(targetNamespace)
          .schema(FIELDNAME_MANIFEST, manifestSchema)
          .schema(FIELDNAME_RECORD, kostraSchemaAdapter.getTargetSchema())
          .build();
    }

    @Override
    public Schema targetAvroSchema() {
        if (targetAvroSchema == null) {
            throw new IllegalStateException("targetAvroSchema is null. Make sure RawdataConverter#init() was invoked in advance.");
        }

        return targetAvroSchema;
    }

    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {
        return true;
    }

    @Override
    public ConversionResult convert(RawdataMessage rawdataMessage) {
        ConversionResultBuilder resultBuilder = ConversionResult.builder(targetAvroSchema, rawdataMessage);

        addManifest(rawdataMessage, resultBuilder);
        convertKostraData(rawdataMessage, resultBuilder);

        return resultBuilder.build();
    }

    void addManifest(RawdataMessage rawdataMessage, ConversionResultBuilder resultBuilder) {
        GenericRecord manifest = new GenericRecordBuilder(manifestSchema)
          .set(FIELDNAME_COLLECTOR, dcManifestSchemaAdapter.newRecord(rawdataMessage, valueInterceptorChain))
          .build();

        resultBuilder.withRecord(FIELDNAME_MANIFEST, manifest);
    }

    void convertKostraData(RawdataMessage rawdataMessage, ConversionResultBuilder resultBuilder) {
        byte[] data = rawdataMessage.get(RAWDATA_ITEMNAME_ENTRY);
        KostraDataEnvelope dataEnvelope = Json.toObject(KostraDataEnvelope.class, new String(data));
        resultBuilder.withRecord(FIELDNAME_RECORD, kostraSchemaAdapter.targetRecordOf(dataEnvelope));
        resultBuilder.appendCounter(MetricName.RAWDATA_RECORDS_TOTAL, 1);
    }

    public static class KostraRawdataConverterException extends RawdataConverterException {
        public KostraRawdataConverterException(String msg) {
            super(msg);
        }
    }

}