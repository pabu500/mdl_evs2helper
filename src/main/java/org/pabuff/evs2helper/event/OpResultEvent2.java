package org.pabuff.evs2helper.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Getter
@Builder
public class OpResultEvent2 {
    private final String itemName;
    private final String opName;
    private final String status;
    private final String error;
    private final String message;
    private final Float progress;
    @JsonProperty("updated_batch_list")
    private final List<Map<String, Object>> updatedBatchList;
}