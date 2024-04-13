package org.pabuff.evs2helper;

import com.pabu5h.evs2.dto.TransactionLogDto;
import org.pabuff.evs2helper.locale.LocalHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PaymentDef {
    public static int offerId = 172;
    public static final int paymentModeVirtual = 4;
    public static final int paymentModeNetOnlineQr = 11;
    public static final int paymentModeENets = 12;
    public static final int paymentModeStripeCard = 21;
    public static final int paymentChannelEvs2Ops = 101; //EVS2 Ops Portal
    public static final int paymentChannelEvs2Consumer = 102; //EVS2 Consumer Portal
    public static double conversionRatio = 10;
    public static int isDedicated = 0;

    @Autowired
    private LocalHelper localHelper;

    public TransactionLogDto buildTransactionDto(String meterDisplayname,
                                                 double topupAmt,
                                                 String remark,
                                                 int paymentMode,
                                                 int transactionStatus,
                                                 int paymentChannel) {
        String localNowStr = localHelper.localSetting().getLocalNowStr();
        String transactionLogTimestamp = localNowStr;
        String responseTimestamp = localNowStr;

        String transactionId = UUID.randomUUID().toString();
        String transactionCode = UUID.randomUUID().toString();

        double gst = localHelper.localSetting().getGST();
        double netAmt = topupAmt / ((100 + gst) / 100);

        boolean completeSendToBackend = true;
        double conversionRate = 10;

        TransactionLogDto transactionLogDto = TransactionLogDto.builder()
                .transactionId(transactionId)
                .transactionLogTimestamp(transactionLogTimestamp)
                .meterDisplayname(meterDisplayname)
                .topupAmt(topupAmt)
                .gst(gst)
                .netAmt(netAmt)
                .paymentMode(paymentMode)
                .currency(localHelper.localSetting().getCurrency())
                .transactionStatus(transactionStatus)
                .offerId(offerId)
                .responseTimestamp(responseTimestamp)
                .completeSendToBackend(completeSendToBackend)
                .transactionCode(transactionCode)
                .paymentChannel(paymentChannel)
                .conversionRatio(conversionRate)
                .auditNo(remark)
                .isDedicated(false)
                .transactionStatusRcved(1)
                .build();

        return transactionLogDto;
    }
}
