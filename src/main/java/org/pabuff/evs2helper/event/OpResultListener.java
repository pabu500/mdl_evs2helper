package org.pabuff.evs2helper.event;

import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class OpResultListener {
//    private final DeferredResult<MeterOpResultEvent> deferredResult = new DeferredResult<>();
    private OpResultEvent opResultEvent;

//    @EventListener
//    public void onMeterOpResultEvent(MeterOpResultEvent event) {
//        System.out.println("Received MeterOp result event - " + event.getMessage());
//        deferredResult.setResult(event);
//    }
    @EventListener
    public void onMeterOpResultEvent(OpResultEvent event) {
//        System.out.println("Received MeterOp result event - " + event.getMessage());
//        deferredResult.setResult(event);
        opResultEvent = event;
    }

    public OpResultEvent getResult(String op) {
        if(op == null || op.isEmpty())
            return opResultEvent == null ? OpResultEvent.builder().build(): opResultEvent;
        if (opResultEvent !=null) {
            if (opResultEvent.getMeterOp()!=null) {
                if(opResultEvent.getMeterOp().equals(op)) {
                    return opResultEvent;
                }
            }
        }
        return OpResultEvent.builder().build();
    }

//    public DeferredResult<MeterOpResultEvent> getDeferredResult() {
//        return deferredResult;
//    }

}
