package no.ssb.rawdata.converter.app.kostra;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dlp.pseudo.core.FieldPseudonymizer;
import no.ssb.rawdata.converter.core.convert.RawdataConverter;
import no.ssb.rawdata.converter.core.convert.RawdataConverterFactory;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import no.ssb.rawdata.converter.core.pseudo.FieldPseudonymizerFactory;
import no.ssb.rawdata.converter.util.Json;

import javax.inject.Singleton;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class DefaultRawdataConverterFactory implements RawdataConverterFactory {
    private final FieldPseudonymizerFactory pseudonymizerFactory;
    private final KostraRawdataConverterConfig defaultRawdataConverterConfig;

    @Override
    public RawdataConverter newRawdataConverter(ConverterJobConfig jobConfig) {
        KostraRawdataConverterConfig converterConfig = defaultRawdataConverterConfig;
        if (! jobConfig.getAppConfig().isEmpty()) {
            try {
                converterConfig = Json.toObject(KostraRawdataConverterConfig.class, jobConfig.appConfigJson());
            }
            catch (Exception e) {
                throw new RawdataConverterException("Invalid KostraRawdataConverterConfig params: " + jobConfig.appConfigJson(), e);
            }
        }

        return newRawdataConverter(jobConfig, converterConfig);
    }

    public RawdataConverter newRawdataConverter(ConverterJobConfig jobConfig, KostraRawdataConverterConfig converterConfig) {
        ValueInterceptorChain valueInterceptorChain = new ValueInterceptorChain();

        if (jobConfig.getPseudoRules() != null && ! jobConfig.getPseudoRules().isEmpty()) {
            FieldPseudonymizer fieldPseudonymizer = pseudonymizerFactory.newFieldPseudonymizer(jobConfig);
            valueInterceptorChain.register(fieldPseudonymizer::pseudonymize);
        }

        // Make sure the converterConfig is not null
        if (converterConfig == null) {
            converterConfig = (defaultRawdataConverterConfig == null) ? new KostraRawdataConverterConfig() : defaultRawdataConverterConfig;
        }

        return new KostraRawdataConverter(converterConfig, valueInterceptorChain);
    }

}