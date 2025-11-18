import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class lstSupplierTrns extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final LoggerAPI logger;
  private final List<Map<String, String>> resultRecords = new ArrayList<>();
  private final Map<String, Double> openingBalances = new HashMap<>();
  private final Map<String, Double> openingDebits = new HashMap<>();
  private final Map<String, Double> openingCredits = new HashMap<>();

  public lstSupplierTrns(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi;
    this.database = database;
    this.program = program;
    this.logger = logger;
  }
  
  public void main() {
    String cono = mi.inData.get("CONO") != null ? mi.inData.get("CONO").trim() : "750";
    String divi = mi.inData.get("DIVI") != null ? mi.inData.get("DIVI").trim() : "ISR";
    String suno = mi.inData.get("SUNO") != null ? mi.inData.get("SUNO").trim() : "";
    String spyn = mi.inData.get("SPYN") != null ? mi.inData.get("SPYN").trim() : "";
    if ((suno == null || suno.isEmpty()) && (spyn == null || spyn.isEmpty())) {
      mi.error("SPYN or SUNO must be entered.");
      return;
    }
    int fdat = mi.inData.get("FDAT") != null && !mi.inData.get("FDAT").trim().isEmpty()
      ? Integer.parseInt(mi.inData.get("FDAT"))
      : 19000101;
    String tdatStr = mi.inData.get("TDAT");
    int tdat;
    if (tdatStr == null || tdatStr.trim().isEmpty()) {
      tdat = getToday();
    } else {
      tdat = Integer.parseInt(tdatStr.trim());
    }
    if (fdat > 19000101) {
      calculateOpeningBalances(cono, divi, spyn, suno, fdat);
    }
    processSupplierTransactions(cono, divi, spyn, suno, fdat, tdat);
    bubbleSort();
    outputResults();
  }
  
  private void calculateOpeningBalances(String cono, String divi, String spyn, String suno, int fdat) {
    ExpressionFactory fpledgExp = database.getExpressionFactory("FPLEDG")
      .eq("EPCONO", cono)
      .and(database.getExpressionFactory("FPLEDG").eq("EPDIVI", divi))
      .and(database.getExpressionFactory("FPLEDG").lt("EPACDT", String.valueOf(fdat)))
      .and(
        database.getExpressionFactory("FPLEDG").eq("EPTRCD", "40")
        .or(database.getExpressionFactory("FPLEDG").eq("EPTRCD", "50"))
      );
  
    if (spyn != null && !spyn.isEmpty()) {
      fpledgExp = fpledgExp.and(database.getExpressionFactory("FPLEDG").eq("EPSPYN", spyn));
    }
    if (suno != null && !suno.isEmpty()) {
      fpledgExp = fpledgExp.and(database.getExpressionFactory("FPLEDG").eq("EPSUNO", suno));
    }
  
    DBAction fpledgQuery = database.table("FPLEDG")
      .index("10")
      .matching(fpledgExp)
      .selection("EPDIVI", "EPSPYN", "EPSUNO", "EPSINO", "EPVSER", "EPVONO",
                 "EPACDT", "EPDUDT", "EPCUAM", "EPTRCD", "EPCONO",
                 "EPYEA4", "EPJRNO", "EPJSNO", "EPIVTP")
      .build();
  
    DBContainer fpledgContainer = fpledgQuery.getContainer();
    fpledgQuery.readAll(fpledgContainer, 0, { DBContainer fpRecord ->
      Map<String, String> record = createBaseRecord(fpRecord);
      String recordSpyn = record.get("SPYN");
      String recordSuno = record.get("SUNO");
      
      if (joinWithFGLEDG(record, cono)) {
        String dbcr = record.get("DBCR");
        double cuam = Math.abs(parseAmount(record.get("CUAM")));//Yael add Math.abs() 06.11.2025. It was in createBaseRecord but I canceled it.
        String key = recordSpyn + "_" + recordSuno;
        
        if ("D".equals(dbcr)) {
            openingBalances.put(key, openingBalances.getOrDefault(key, 0.0d) + cuam);
            openingDebits.put(key, openingDebits.getOrDefault(key, 0.0d) + cuam);
        } else if ("C".equals(dbcr)) {
            openingBalances.put(key, openingBalances.getOrDefault(key, 0.0d) - cuam);
            openingCredits.put(key, openingCredits.getOrDefault(key, 0.0d) + cuam);
        }
      }
    });
  }
  
  private void processSupplierTransactions(String cono, String divi, String spyn, String suno, int fdat, int tdat) {
    ExpressionFactory fpledgExp = database.getExpressionFactory("FPLEDG")
      .eq("EPCONO", cono)
      .and(database.getExpressionFactory("FPLEDG").eq("EPDIVI", divi))
      .and(database.getExpressionFactory("FPLEDG").ge("EPACDT", String.valueOf(fdat)))
      .and(database.getExpressionFactory("FPLEDG").le("EPACDT", String.valueOf(tdat)))
      .and(
        database.getExpressionFactory("FPLEDG").eq("EPTRCD", "40")
        .or(database.getExpressionFactory("FPLEDG").eq("EPTRCD", "50"))
      );
      
    if (spyn != null && !spyn.isEmpty()) {
      fpledgExp = fpledgExp.and(database.getExpressionFactory("FPLEDG").eq("EPSPYN", spyn));
    }
    if (suno != null && !suno.isEmpty()) {
      fpledgExp = fpledgExp.and(database.getExpressionFactory("FPLEDG").eq("EPSUNO", suno));
    }
      
    DBAction fpledgQuery = database.table("FPLEDG")
      .index("10")
      .matching(fpledgExp)
      .selection("EPDIVI", "EPSPYN", "EPSUNO", "EPSINO", "EPVSER", "EPVONO",
                 "EPACDT", "EPDUDT", "EPCUAM", "EPTRCD", "EPCONO",
                 "EPYEA4", "EPJRNO", "EPJSNO", "EPIVTP", "EPIVDT")
      .build();
      
    DBContainer fpledgContainer = fpledgQuery.getContainer();
    fpledgQuery.readAll(fpledgContainer, 0, { DBContainer fpRecord ->
      Map<String, String> record = createBaseRecord(fpRecord);
      if (joinWithFGLEDG(record, cono)) {
        String trcd = record.get("TRCD");//Yael 06/11/2025.
          joinWithFPLEDX(record, cono, "409");
          joinWithFPLEDX(record, cono, "406");
          joinWithFPLEDX(record, cono, "450");
          joinWithFPLEDX(record, cono, "404");//Yael 04/11/2025.
        joinWithCSYNBV(record, cono);
        //joinWithFAPCHK(record, cono);//Yael 04/11/2025.
        //joinWithCSYTAB(record, cono, suno);//Yael 04/11/2025.
        calculateDebitCredit(record);
        setAsmakhtaValues(record);
        resultRecords.add(record);
      }
    });
  }

  private Map<String, String> createBaseRecord(DBContainer fpRecord) {
    Map<String, String> record = new HashMap<>();
    record.put("DIVI", String.valueOf(fpRecord.get("EPDIVI")));
    record.put("SPYN", String.valueOf(fpRecord.get("EPSPYN")));
    record.put("SUNO", String.valueOf(fpRecord.get("EPSUNO")));
    record.put("SINO", String.valueOf(fpRecord.get("EPSINO")));
    record.put("VSER", String.valueOf(fpRecord.get("EPVSER")));
    record.put("VONO", String.valueOf(fpRecord.get("EPVONO")));
    record.put("ACDT", String.valueOf(fpRecord.get("EPACDT")));
    record.put("DUDT", String.valueOf(fpRecord.get("EPDUDT")));
    double cuam = (Double)fpRecord.get("EPCUAM");
    //record.put("CUAM", formatAmount(Math.abs(cuam)));
    record.put("CUAM", formatAmount(cuam));//Yael remove Math.abs(cuam) 04/11/2025
    record.put("TRCD", String.valueOf(fpRecord.get("EPTRCD")));
    record.put("YEA4", String.valueOf(fpRecord.get("EPYEA4")));
    record.put("JRNO", String.valueOf(fpRecord.get("EPJRNO")));
    record.put("JSNO", String.valueOf(fpRecord.get("EPJSNO")));
    record.put("IVTP", String.valueOf(fpRecord.get("EPIVTP")));
    record.put("RMSV", "");
    record.put("PMSV", "");
    record.put("CHKN", "");
    record.put("TX15", "");
    record.put("DBIT", "0.00");
    record.put("CDIT", "0.00");
    record.put("FEID", "");
    record.put("FNCN", "");
    record.put("VTXT", "");
    record.put("ACAM", "0.00");
    record.put("TCAM", "0.00");
    record.put("DBCR", "");
    record.put("CUCD", "");
    record.put("ASM1", ""); 
    record.put("ASM2", ""); 
    //record.put("TX40", "");//Yael 04/11/2025
    record.put("IVDT", String.valueOf(fpRecord.get("EPIVDT")));
    return record;
  }
  
  private boolean joinWithFGLEDG(Map<String, String> record, String cono) {
    String yea4 = record.get("YEA4");
    String jrno = record.get("JRNO");
    String jsno = record.get("JSNO");
    String trcd = record.get("TRCD");
    String ivtp = record.get("IVTP");
    String divi = record.get("DIVI");
    ExpressionFactory fgledgExp = database.getExpressionFactory("FGLEDG")
      .eq("EGCONO", cono)
      .and(database.getExpressionFactory("FGLEDG").eq("EGDIVI", divi))
      .and(database.getExpressionFactory("FGLEDG").eq("EGYEA4", yea4))
      .and(database.getExpressionFactory("FGLEDG").eq("EGJRNO", jrno))
      .and(database.getExpressionFactory("FGLEDG").eq("EGJSNO", jsno));
    DBAction fgledgQuery = database.table("FGLEDG")
      .index("00")
      .matching(fgledgExp)
      .selection("EGACAM", "EGTCAM", "EGDBCR", "EGCUCD", "EGFEID", "EGFNCN", "EGVTXT", "EGVDSC")
      .build();
    DBContainer fgledgContainer = fgledgQuery.getContainer();
    final boolean[] found = [false];
    fgledgQuery.readAll(fgledgContainer, 0, { DBContainer fgRecord ->
      String feid = String.valueOf(fgRecord.get("EGFEID"));
      String fncn = String.valueOf(fgRecord.get("EGFNCN"));
      boolean isValid = false;
      if ("40".equals(trcd) && ("AP10".equals(feid) || "AP50".equals(feid))) {//Yael add || "AP50".equals(feid). 04/11/2025
        isValid = true;
      } else if ("40".equals(trcd) && ("AP".equals(ivtp) || "PR".equals(ivtp))) {
        isValid = true;
      } else if ("50".equals(trcd) && !"500".equals(fncn)) {
        isValid = true;
      }
      if (isValid) {
        record.put("DBCR", String.valueOf(fgRecord.get("EGDBCR")));
        record.put("CUCD", String.valueOf(fgRecord.get("EGCUCD")));
        double acam = (Double)fgRecord.get("EGACAM");
        double tcam = (Double)fgRecord.get("EGTCAM");
        record.put("ACAM", formatAmount(acam));
        record.put("TCAM", formatAmount(tcam));
        record.put("FEID", feid);
        record.put("FNCN", fncn);
        record.put("VTXT", String.valueOf(fgRecord.get("EGVTXT")));
        record.put("VDSC", String.valueOf(fgRecord.get("EGVDSC")));//Yael 04/11/2025
        found[0] = true;
      }
    });
    return found[0];
  }

  private void joinWithFPLEDX(Map<String, String> record, String cono, String pexn) {
    /*if (!"50".equals(record.get("TRCD"))) {
      return;
    }*///Yael. 06.11.2025
    String divi = record.get("DIVI");
    String yea4 = record.get("YEA4");
    String jrno = record.get("JRNO");
    String jsno = record.get("JSNO");
    ExpressionFactory fpledxExp = database.getExpressionFactory("FPLEDX")
      .eq("EPCONO", cono)
      .and(database.getExpressionFactory("FPLEDX").eq("EPDIVI", divi))
      .and(database.getExpressionFactory("FPLEDX").eq("EPYEA4", yea4))
      .and(database.getExpressionFactory("FPLEDX").eq("EPJRNO", jrno))
      .and(database.getExpressionFactory("FPLEDX").eq("EPJSNO", jsno))
      .and(database.getExpressionFactory("FPLEDX").eq("EPPEXN", pexn));
    DBAction fpledxQuery = database.table("FPLEDX")
      .index("00")
      .matching(fpledxExp)
      .selection("EPPEXI")
      .build();
    DBContainer fpledxContainer = fpledxQuery.getContainer();
    fpledxQuery.readAll(fpledxContainer, 0, { DBContainer xRecord ->
      String pexi = String.valueOf(xRecord.get("EPPEXI")).trim();//Yael add trim(). 06.11.2025
      if ("409".equals(pexn)) {
        if (pexi.length() > 10) {
          record.put("RMSV", pexi.substring(pexi.length() - 10));
        } else {
          record.put("RMSV", pexi);
        }
      } else if ("406".equals(pexn)) {
        record.put("PMSV", pexi);
      } else if ("450".equals(pexn)) {
        if (pexi.length() > 10) {
          record.put("RMSV", pexi.substring(pexi.length() - 10));
        } else {
          record.put("RMSV", pexi);
        }
      }else if("404".equals(pexn)){//Yael 04/11/2025.
         if (pexi.length() > 10) {
           record.put("CHKN", pexi.substring(pexi.length() - 10));
         }else{
           record.put("CHKN", pexi);
         }
      }
    });
  }

  private void joinWithCSYNBV(Map<String, String> record, String cono) {
    String divi = record.get("DIVI");
    String vser = record.get("VSER");
    ExpressionFactory csynbvExp = database.getExpressionFactory("CSYNBV")
      .eq("DVCONO", cono)
      .and(database.getExpressionFactory("CSYNBV").eq("DVDIVI", divi))
      .and(database.getExpressionFactory("CSYNBV").eq("DVVSER", vser));
    DBAction csynbvQuery = database.table("CSYNBV")
      .matching(csynbvExp)
      .selection("DVTX15")
      .build();
    DBContainer csynbvContainer = csynbvQuery.getContainer();
    csynbvQuery.readAll(csynbvContainer, 0, { DBContainer cRecord ->
      record.put("TX15", String.valueOf(cRecord.get("DVTX15")));
    });
  }

  /*private void joinWithFAPCHK(Map<String, String> record, String cono) {
    String divi = record.get("DIVI");
    String vser = record.get("VSER");
    String vono = record.get("VONO");
    ExpressionFactory fapchkExp = database.getExpressionFactory("FAPCHK")
      .eq("CKCONO", cono)
      .and(database.getExpressionFactory("FAPCHK").eq("CKDIVI", divi))
      .and(database.getExpressionFactory("FAPCHK").eq("CKVSER", vser))
      .and(database.getExpressionFactory("FAPCHK").eq("CKVONO", vono));
    DBAction fapchkQuery = database.table("FAPCHK")
      .index("00")
      .matching(fapchkExp)
      .selection("CKCHKN")
      .build();
    DBContainer fapchkContainer = fapchkQuery.getContainer();
    fapchkQuery.readAll(fapchkContainer, 0, { DBContainer cRecord ->
      String chkn = String.valueOf(cRecord.get("CKCHKN"));
      if (chkn != null && !chkn.trim().isEmpty() && !"null".equals(chkn)) {
        record.put("CHKN", chkn);
      }
    });
  }*///Yael 04/11/2025
  
  /*private void joinWithCSYTAB(Map<String, String> record, String cono, String sunoParam) {
    String feid = record.get("FEID");
    String fncn = record.get("FNCN");
    if (fncn.length() < 3) {
      fncn = String.format("%03d", Integer.parseInt(fncn));
    }
    String divi = record.get("DIVI");
    if (feid == null || feid.isEmpty() || fncn == null || fncn.isEmpty()) {
        return;
    }
    String stky = feid + fncn;
    ExpressionFactory csytabExp = database.getExpressionFactory("CSYTAB").eq("CTCONO", cono)
        .and(database.getExpressionFactory("CSYTAB").eq("CTDIVI", divi))
        .and(database.getExpressionFactory("CSYTAB").eq("CTSTCO", "FFNC"))
        .and(database.getExpressionFactory("CSYTAB").eq("CTSTKY", stky));
    DBAction csytabQuery = database.table("CSYTAB")
        .matching(csytabExp)
        .selection("CTTX40")
        .build();
    DBContainer csytabContainer = csytabQuery.getContainer();
    csytabQuery.readAll(csytabContainer, 0, { DBContainer csRecord ->
        String tx40 = String.valueOf(csRecord.get("CTTX40"));
        if (tx40 != null && !tx40.trim().isEmpty() && !"null".equals(tx40)) {
            record.put("TX40", tx40);
        }
    });
  }*///Yael 04/11/2025

  private void calculateDebitCredit(Map<String, String> record) {
    String dbcr = record.get("DBCR");
    String cuam = record.get("CUAM");
    
    // Handle nulls and parse safely
    double cuamDouble = 0.0;
    if (cuam != null && !cuam.isEmpty()) {
        cuamDouble = Double.parseDouble(cuam);
    }//Yael. 06.11.2025
    
    if ("D".equals(dbcr)) {
      //record.put("DBIT", cuam);
       record.put("DBIT", String.format("%.2f", cuamDouble));//Yael. 06.11.2025
      record.put("CDIT", "0.00");
    } else if ("C".equals(dbcr)) {
      record.put("DBIT", "0.00");
      //record.put("CDIT", cuam);
      record.put("CDIT", String.format("%.2f", cuamDouble * -1));//Yael. 06.11.2025
    }
  }

  private boolean shouldSwap(Map<String, String> record1, Map<String, String> record2) {
    String spyn1 = record1.get("SPYN");
    String spyn2 = record2.get("SPYN");
    int spynCompare = spyn1.compareTo(spyn2);
    if (spynCompare != 0) {
      return spynCompare > 0;
    }
    int acdt1 = Integer.parseInt(record1.get("ACDT"));
    int acdt2 = Integer.parseInt(record2.get("ACDT"));
    if (acdt1 != acdt2) {
      return acdt1 > acdt2;
    }
    String vono1 = record1.get("VONO");
    String vono2 = record2.get("VONO");
    return vono1.compareTo(vono2) > 0;
  }
  
  private Map<String, String> createSummaryRecord(Map<String, String> baseRecord) {
    Map<String, String> summaryRecord = new HashMap<>(baseRecord);
    summaryRecord.put("VONO", "");
    summaryRecord.put("SINO", "");
    summaryRecord.put("CHKN", "");
    summaryRecord.put("RMSV", "");
    summaryRecord.put("PMSV", "");
    summaryRecord.put("ACDT", "");
    summaryRecord.put("DUDT", "");
    summaryRecord.put("TRCD", "");
    summaryRecord.put("FEID", "");
    summaryRecord.put("FNCN", "");
    summaryRecord.put("ASM1", "");
    summaryRecord.put("ASM2", ""); 
    return summaryRecord;
  }
  
  private void outputRecord(Map<String, String> record) {
    mi.outData.put("DIVI", record.get("DIVI"));
    mi.outData.put("SPYN", record.get("SPYN"));
    mi.outData.put("SUNO", record.get("SUNO"));
    mi.outData.put("SINO", record.get("SINO"));
    mi.outData.put("VSER", record.get("VSER"));
    mi.outData.put("VONO", record.get("VONO"));
    mi.outData.put("ACDT", record.get("ACDT"));
    mi.outData.put("DUDT", record.get("DUDT"));
    mi.outData.put("CUAM", record.get("CUAM"));
    mi.outData.put("DBCR", record.get("DBCR"));
    mi.outData.put("CUCD", record.get("CUCD"));
    mi.outData.put("RMSV", record.get("RMSV"));
    mi.outData.put("PMSV", record.get("PMSV"));
    mi.outData.put("CHKN", stripLeadingZeros(record.get("CHKN")));
    mi.outData.put("TRCD", record.get("TRCD"));
    mi.outData.put("FEID", record.get("FEID"));
    mi.outData.put("FNCN", record.get("FNCN"));
    mi.outData.put("VTXT", record.get("VTXT"));
    mi.outData.put("ACAM", record.get("ACAM"));
    mi.outData.put("TCAM", record.get("TCAM"));
    mi.outData.put("TX15", record.get("TX15"));
    mi.outData.put("DBIT", record.get("DBIT"));
    mi.outData.put("CDIT", record.get("CDIT"));
    mi.outData.put("LABL", record.get("LABL"));
    mi.outData.put("IVTP", record.get("IVTP"));
    mi.outData.put("BLNC", record.get("BLNC"));
    mi.outData.put("ASM1", record.get("ASM1"));
    mi.outData.put("ASM2", record.get("ASM2"));
    //mi.outData.put("TX40", record.get("TX40"));//Yael 04/11/2025
    mi.outData.put("IVDT", record.get("IVDT"));
    mi.outData.put("VDSC", record.get("VDSC"));
    mi.write();
  }
  
  private String stripLeadingZeros(String value) {
    if (value == null) return null
    return value.replaceFirst('^0+(?!$)', '')
  }
  
  private double parseAmount(String amount) {
    try {
      return Double.parseDouble(amount);
    } catch (NumberFormatException e) {
      return 0.0;
    }
  }

  private int getToday() {
    LocalDate today = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    return Integer.parseInt(today.format(formatter));
  }

  private String formatAmount(double amount) {
    return String.format("%.2f", amount);
  }
  
  private void bubbleSort() {
    int n = resultRecords.size();
    for (int i = 0; i < n - 1; i++) {
      for (int j = 0; j < n - i - 1; j++) {
        if (shouldSwap(resultRecords.get(j), resultRecords.get(j + 1))) {
          Map<String, String> temp = resultRecords.get(j);
          resultRecords.set(j, resultRecords.get(j + 1));
          resultRecords.set(j + 1, temp);
        }
      }
    }
  }
  
  private void outputResults() {
    if (resultRecords.isEmpty()) {
      return;
    }
    Map<String, Map<String, List<Map<String, String>>>> groupedData = new LinkedHashMap<>();
    for (Map<String, String> record : resultRecords) {
      String spyn = record.get("SPYN");
      String suno = record.get("SUNO");
      groupedData.computeIfAbsent(spyn, {k -> new LinkedHashMap<>()})
                 .computeIfAbsent(suno, {k -> new ArrayList<>()})
                 .add(record);
    }
    
    for (String spyn : groupedData.keySet()) {
      Map<String, List<Map<String, String>>> sunoGroups = groupedData.get(spyn);
      double spynTotalDebit = 0.0;
      double spynTotalCredit = 0.0;
      for (String suno : sunoGroups.keySet()) {
        List<Map<String, String>> transactions = sunoGroups.get(suno);
        if (transactions.isEmpty()) continue;
        String key = spyn + "_" + suno;
        double runningBalance = openingBalances.getOrDefault(key, 0.0d);
        Map<String, String> openingRecord = createOpeningBalanceRecord(transactions.get(0), runningBalance);
        openingRecord.put("LABL", "OPENING_BALANCE");
        openingRecord.put("BLNC", formatAmount(runningBalance));
        outputRecord(openingRecord);
        double transactionDebitTotal = 0.0;
        double transactionCreditTotal = 0.0;
        for (Map<String, String> record : transactions) {
          record.put("LABL", "TRANSACTION");
          double debit = parseAmount(record.get("DBIT"));
          double credit = parseAmount(record.get("CDIT"));
          runningBalance = runningBalance + debit - credit;
          record.put("BLNC", formatAmount(runningBalance)); 
          transactionDebitTotal += debit;
          transactionCreditTotal += credit;
          outputRecord(record);
        }
        double openingDebit = openingDebits.getOrDefault(key, 0.0d);
        double openingCredit = openingCredits.getOrDefault(key, 0.0d);
        double totalDebit = openingDebit + transactionDebitTotal;
        double totalCredit = openingCredit + transactionCreditTotal;
        Map<String, String> closingRecord = createSummaryRecord(transactions.get(transactions.size() - 1));
        closingRecord.put("DBIT", formatAmount(totalDebit));             
        closingRecord.put("CDIT", formatAmount(totalCredit));              
        closingRecord.put("CUAM", formatAmount(Math.abs(totalDebit - totalCredit)));
        closingRecord.put("BLNC", formatAmount(runningBalance));              
        closingRecord.put("VTXT", "Closing Balance");
        closingRecord.put("LABL", "CLOSING_BALANCE");
        outputRecord(closingRecord);
        if (sunoGroups.size() > 1) {
          Map<String, String> sunoSummary = createSummaryRecord(transactions.get(0));
          sunoSummary.put("DBIT", formatAmount(totalDebit));
          sunoSummary.put("CDIT", formatAmount(totalCredit));
          sunoSummary.put("CUAM", formatAmount(Math.abs(totalDebit - totalCredit)));
          sunoSummary.put("BLNC", formatAmount(runningBalance));
          sunoSummary.put("VTXT", "Total for supplier " + suno);
          sunoSummary.put("LABL", "SUPPLIER_TOTAL");
          outputRecord(sunoSummary);
        }
        spynTotalDebit += totalDebit
        spynTotalCredit += totalCredit; 
      }
      if (groupedData.size() > 1) {
        Map<String, String> spynSummary = createSummaryRecord(
          groupedData.get(spyn).values().iterator().next().get(0)
        );
        spynSummary.put("DBIT", formatAmount(spynTotalDebit));
        spynSummary.put("CDIT", formatAmount(spynTotalCredit));
        spynSummary.put("CUAM", formatAmount(Math.abs(spynTotalDebit - spynTotalCredit)));
        spynSummary.put("VTXT", "Total for beneficiary " + spyn);
        spynSummary.put("LABL", "BENEFICIARY_TOTAL");
        outputRecord(spynSummary);
      }
    }
  }
  
  private Map<String, String> createOpeningBalanceRecord(Map<String, String> baseRecord, double balance) {
    Map<String, String> openingRecord = new HashMap<>(baseRecord);
    openingRecord.put("VONO", "");
    openingRecord.put("SINO", "");
    openingRecord.put("CHKN", "");
    openingRecord.put("RMSV", "");
    openingRecord.put("PMSV", "");
    openingRecord.put("ACDT", "");
    openingRecord.put("DUDT", "");
    openingRecord.put("VTXT", "Opening Balance");
    openingRecord.put("ASM1", ""); 
    openingRecord.put("ASM2", "");
    openingRecord.put("TRCD", "");
    openingRecord.put("FEID", "");
    openingRecord.put("FNCN", "");
    openingRecord.put("ACAM", "");
    openingRecord.put("TCAM", "");
    openingRecord.put("TX15", "");
    String spyn = baseRecord.get("SPYN");
    String suno = baseRecord.get("SUNO");
    String key = spyn + "_" + suno;
    double totalDebit = openingDebits.getOrDefault(key, 0.0d);
    double totalCredit = openingCredits.getOrDefault(key, 0.0d);
    openingRecord.put("DBIT", formatAmount(totalDebit));
    openingRecord.put("CDIT", formatAmount(totalCredit));
    if (balance >= 0) {
      openingRecord.put("DBCR", "D");
    } else {
      openingRecord.put("DBCR", "C");
    }
    openingRecord.put("CUAM", formatAmount(Math.abs(balance)));
    return openingRecord;
  }
    
  private void setAsmakhtaValues(Map<String, String> record) {
    String chkn = record.get("CHKN");
    String pmsv = record.get("PMSV");       
    String rmsv = record.get("RMSV");
    String originalSino = record.get("SINO"); 
    //Yael. 18/11/2025
    String feid = record.get("FEID");
    if(feid != "AP50" && feid != "AP10"){
      if (chkn != null && !chkn.trim().isEmpty() && !"null".equals(chkn)) {
          record.put("ASM1", stripLeadingZeros(chkn));
      }else if (rmsv != null && !rmsv.trim().isEmpty() && !"null".equals(rmsv)) {
        record.put("ASM1", rmsv);
      } else if (pmsv != null && !pmsv.trim().isEmpty() && !"null".equals(pmsv)) {
        record.put("ASM1", pmsv);
      }
      record.put("ASM2", originalSino);
    }else{
      if (originalSino != null && !originalSino.trim().isEmpty() && !"null".equals(originalSino)) {
            record.put("ASM1", originalSino);
        } else {
            record.put("ASM1", "");
        }
    }
    /*if (chkn != null && !chkn.trim().isEmpty() && !"null".equals(chkn)) {
        record.put("ASM1", stripLeadingZeros(chkn));
        record.put("ASM2", originalSino);
    } else if (rmsv != null && !rmsv.trim().isEmpty() && !"null".equals(rmsv)) {
        record.put("ASM1", rmsv);
        record.put("ASM2", originalSino);
    } else if (pmsv != null && !pmsv.trim().isEmpty() && !"null".equals(pmsv)) {
        record.put("ASM1", pmsv);
        record.put("ASM2", originalSino);
    } else {
        if (originalSino != null && !originalSino.trim().isEmpty() && !"null".equals(originalSino)) {
            record.put("ASM1", originalSino);
            record.put("ASM2", "");
        } else {
            record.put("ASM1", "");
            record.put("ASM2", "");
        }
    }*/
  }
}