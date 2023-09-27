package com.pabu5h.evs2.evs2helper;

final record LocalSettingSG() {
    static String countryCode = "SG";
    static String timeZone = "Asia/Singapore";
    static String timeZoneOffset = "+08:00";
    static String currency = "SGD";
    static String language = "en";
    static double gst = 8;

    static int offerId = 172;
    static final int paymentModeNetOnlineQr = 11;
    static final int paymentModeENets = 12;
    static final int paymentModeStripeCard = 21;
    static final int paymentChannel = 102; //EVS2 Consumer Portal
    static double conversionRatio = 10;
    static int isDedicated = 0;
}
