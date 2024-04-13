package org.pabuff.evs2helper.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Getter
@Builder
public class OpResultEvent {
    private final String meterSn;
    private final String meterOp;
    private final String status;
    private final String error;
    private final String message;
    private final List<Map<String, Object>> updatedBatchList;
}
