package com.pabu5h.evs2.evs2helper.scope;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class SmrtHelper {

    List<Station> stationsEW = new ArrayList<>();
    List<Station> stationsNS = new ArrayList<>();
    List<Station> stationsCC = new ArrayList<>();
    List<Station> stationsTE = new ArrayList<>();

    public SmrtHelper() {
        stationsEW.add(new Station("EW1", "Pasir Ris", "smrt_pasir_ris"));
        stationsEW.add(new Station("EW2", "Tampines", "smrt_tampines"));
        stationsEW.add(new Station("EW3", "Simei", "smrt_simei"));
        stationsEW.add(new Station("EW4", "Tanah Merah", "smrt_tanah_merah"));
        stationsEW.add(new Station("EW5", "Bedok", "smrt_bedok"));
        stationsEW.add(new Station("EW6", "Kembangan", "smrt_kembangan"));
        stationsEW.add(new Station("EW7", "Eunos", "smrt_eunos"));
        stationsEW.add(new Station("EW8", "Paya Lebar", "smrt_paya_lebar"));
        stationsEW.add(new Station("EW9", "Aljunied", "smrt_aljunied"));
        stationsEW.add(new Station("EW10", "Kallang", "smrt_kallang"));
        stationsEW.add(new Station("EW11", "Lavender", "smrt_lavender"));
        stationsEW.add(new Station("EW12", "Bugis", "smrt_bugis"));
        stationsEW.add(new Station("EW13", "City Hall", "smrt_city_hall"));
        stationsEW.add(new Station("EW14", "Raffles Place", "smrt_raffles_place"));
        stationsEW.add(new Station("EW15", "Tanjong Pagar", "smrt_tanjong_pagar"));
        stationsEW.add(new Station("EW16", "Outram Park", "smrt_outram_park"));
        stationsEW.add(new Station("EW17", "Tiong Bahru", "smrt_tiong_bahru"));
        stationsEW.add(new Station("EW18", "Redhill", "smrt_redhill"));
        stationsEW.add(new Station("EW19", "Queenstown", "smrt_queenstown"));
        stationsEW.add(new Station("EW20", "Commonwealth", "smrt_commonwealth"));
        stationsEW.add(new Station("EW21", "Buona Vista", "smrt_buona_vista"));
        stationsEW.add(new Station("EW22", "Dover", "smrt_dover"));
    }
}

@Getter
record Station(String code, String name, String siteTag) {
}
