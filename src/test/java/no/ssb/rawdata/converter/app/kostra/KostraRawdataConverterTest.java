package no.ssb.rawdata.converter.app.kostra;

import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.test.message.RawdataMessageFixtures;
import no.ssb.rawdata.converter.test.message.RawdataMessages;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Set;

@Disabled
public class KostraRawdataConverterTest {

    static RawdataMessageFixtures fixtures;

    @BeforeAll
    static void loadFixtures() {
        fixtures = RawdataMessageFixtures.init("sometopic");
    }

    @Disabled
    @Test
    void shouldConvertRawdataMessages() {
        RawdataMessages messages = fixtures.rawdataMessages("sometopic"); // TODO: replace with topicname
        KostraRawdataConverterConfig config = new KostraRawdataConverterConfig();
        // TODO: Set app config

        KostraRawdataConverter converter = new KostraRawdataConverter(config, new ValueInterceptorChain());
        converter.init(messages.index().values());
        ConversionResult res = converter.convert(messages.index().get("123456")); // TODO: replace with message position
    }

}
