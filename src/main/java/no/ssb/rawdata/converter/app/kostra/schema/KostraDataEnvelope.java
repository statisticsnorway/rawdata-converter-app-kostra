package no.ssb.rawdata.converter.app.kostra.schema;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class KostraDataEnvelope {

    private List<StructureItem> structure;
    private List<DataItem> data;

    public DataItem getDataItem() {
        return data.stream().findFirst().orElseThrow(() -> new IllegalStateException("No data item"));
    }

    @Data
    public static class StructureItem {
        private String name;
        private String type;
        private String role;
    }

    public static class DataItem extends ArrayList<Object> {
    }

}
