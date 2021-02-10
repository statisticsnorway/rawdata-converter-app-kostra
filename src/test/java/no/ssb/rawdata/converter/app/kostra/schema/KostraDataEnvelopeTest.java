package no.ssb.rawdata.converter.app.kostra.schema;

import no.ssb.dlp.pseudo.core.util.Json;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class KostraDataEnvelopeTest {

    String json = """
      {
        "structure": [
          {
            "name": "aaa",
            "role": "IDENTIFIER",
            "type": "STRING"
          },
          {
            "name": "bbb",
            "role": "IDENTIFIER",
            "type": "STRING"
          },
          {
            "name": "ccc",
            "role": "IDENTIFIER",
            "type": "STRING"
          },
          {
            "name": "ddd",
            "role": "IDENTIFIER",
            "type": "STRING"
          },
          {
            "name": "eee",
            "role": "IDENTIFIER",
            "type": "STRING"
          },
          {
            "name": "fff",
            "role": "IDENTIFIER",
            "type": "STRING"
          },
          {
            "name": "ggg",
            "role": "MEASURE",
            "type": "INTEGER"
          }
        ],
        "data": [
          [
            "123",
            "123",
            "1234",
            "1",
            "2018",
            "B",
            321
          ]
        ]
      }
      """;

    @Test
    void parseFromJson() {
        KostraDataEnvelope envelope = Json.toObject(KostraDataEnvelope.class, json);
        assertThat(envelope.getStructure()).hasSize(7);
        assertThat(envelope.getData()).hasSize(1);
    }
}