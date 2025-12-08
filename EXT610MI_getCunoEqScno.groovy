public class getCunoEqScno extends ExtendM3Transaction {
    private final MIAPI mi;
    private final DatabaseAPI database;
    private final ProgramAPI program;
    private final UtilityAPI utility;
    
    private static final String[] CIDVEN_FIELDS = ["IICONO", "IISUNO", "IISUCL", "IIBUYE", "IIRESP", "IIQUCL", "IIABSK", "IIABSM", "IISCNO", "IIOUCN",
        "IIPRSU", "IICOBI", "IIAGNT", "IIORTY", "IICUCD", "IICRTP", "IIVTCD", "IITXAP", "IIDCSM", "IITEPY",
        "IIPYME", "IITECD", "IITEDL", "IIMODL", "IITEPA", "IITEAF", "IIDT4T", "IIPACD", "IIIAPC", "IIIAPE",
        "IIIAPF", "IIIAPT", "IIFUSC", "IISPFC", "IISHST", "IIPSTM", "IISUST", "IISUSY", "IIREGR", "IIACRF",
        "IICFI1", "IICFI2", "IICFI3", "IICFI4", "IICFI5", "IIPTDY", "IIDTDY", "IILIDT", "IISHAC", "IIGRPY",
        "IISERS", "IISUGR", "IITAXC", "IIPWMT", "IITXID", "IITDCD", "IIPOOT", "IIATPR", "IIDTCD", "IISBPE",
        "IITAME", "IIWIPR", "IITIDN", "IITINO", "IIAVCD", "IICINP", "IISCIS", "IIRGDT", "IIRGTM", "IILMDT",
        "IICHNO", "IICHID", "IILMTS", "IIPPLV", "IICGRP", "IITXIN", "IIDTID", "IIALCS", "IIIFAC", "IISUWH",
        "IIAUCD", "IISRAM", "IISCIP", "IISCIR", "IIAUTV"
    ];

    public getCunoEqScno(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility) {
        this.mi = mi;
        this.database = database;
        this.program = program;
        this.utility = utility;
    }

    public void main() {
      
        readCIDVEN();
      
    }

    void readCIDVEN() {
        ExpressionFactory exp = database.getExpressionFactory("CIDVEN");
        exp = exp.eq("IICONO", mi.inData.get("CONO"));
        DBAction action = database.table("CIDVEN")
                .index("00")
                .matching(exp)
                .selection(
                    CIDVEN_FIELDS
                )
                .build();
        DBContainer container = action.getContainer();
        action.readAll(container, 0, 10000, {DBContainer data -> 
            Map<String, String> record = new HashMap<>();
            for (String field : CIDVEN_FIELDS) {
                Object value = data.get(field);
                record.put(field, value != null ? value.toString() : "");
            }
            joinOCUSMA(record);
        });
    }
    

    void joinOCUSMA(Map<String, String> record) {
        String scno = record.get("IISCNO");
        if (scno == null || scno.isEmpty()) {
            return;
        }
        
        ExpressionFactory exp = database.getExpressionFactory("OCUSMA");
        exp = exp.eq("OKCUNO", scno); 
        if (mi.inData.get("SRCH")?.trim()) {
          List<String> splittedData = Arrays.asList(mi.inData.get("SRCH").trim().split(":"));
          String searchField = "OK" + splittedData.get(0).trim();
          String searchValue = splittedData.get(1).trim() + "%";
          ExpressionFactory newExp = database.getExpressionFactory("OCUSMA").like(searchField, searchValue);
          exp = exp.and(newExp);
        }
        DBAction action = database.table("OCUSMA")
                .index("00")
                .matching(exp)
                .selection("OKCUNO", "OKCUNM") 
                .build();

        DBContainer container = action.getContainer();

        action.readAll(container, 0, 1, {DBContainer data -> 
            for (String field : CIDVEN_FIELDS) {
                String shortName = field.length() > 2 ? field.substring(2) : field; // убираем первые 2 буквы
                String value = record.get(field) != null ? record.get(field) : "";
                mi.outData.put(shortName, value);
            }
            
            
            mi.outData.put("CUNO", data.get("OKCUNO").toString());
            mi.outData.put("CUNM", data.get("OKCUNM").toString());
            mi.write();
        });
    }
}