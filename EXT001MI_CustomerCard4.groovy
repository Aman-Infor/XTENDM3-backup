import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class CustomerCard4 extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final LoggerAPI logger;
  
  String fromCustomer;
  String toCustomer;
  String payer;
  String fromDate;
  String toDate;
  boolean isPayer;
  boolean isFromCust;
  boolean isToCust;
  List<Map<String,String>> fsledgList;
  List<Map<String,String>> fgledgList;
  List<Map<String,String>> fdrfmaList;
  List<Map<String,String>> csynbvList;
  List<Map<String,String>> ocusmaList;
  public CustomerCard4(MIAPI mi,DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi;
    this.database = database;
    this.program = program;
    this.logger = logger;
  }

  public void main() {
    payer=mi.inData.get("PYNO").trim();
    fromCustomer=mi.inData.get("FCUS").trim();
    toCustomer=mi.inData.get("TCUS").trim();
    isPayer=true;
    isFromCust=true;
    isToCust=true;
    if(payer==null||payer.equals("")){
      isPayer=false;
    }
    if(fromCustomer==null||fromCustomer.equals("")){
      isFromCust=false;
    }
    if(toCustomer==null||toCustomer.equals("")){
      isToCust=false;
      
    }
    if((!isPayer&&!isFromCust&&!isToCust)){
      this.mi.error("customer must be entered");
      return;
    }
    fromDate=mi.inData.get("FDAT").trim();
    toDate=mi.inData.get("TDAT").trim();
    /*if(fromDate==null||fromDate.equals("")){
      fromDate=getStartYearDate();
    }*/
    if(toDate==null||toDate.equals("")){
      toDate=getCurrentDate();
    }
    fsledgList=new ArrayList<>();
    fgledgList=new ArrayList<>();
    fdrfmaList=new ArrayList<>();
    csynbvList=new ArrayList<>();
    ocusmaList=new ArrayList<>();
    readRecordsFSLEDG();
    //sortTable(fsledgList,"ACDT");
    bubbleSort(fsledgList,"ACDT");
    readRecordsFGLEDG();
    readRecordsFDRFMA();
    readRecordsCSYNBV();
    
    
    if(fsledgList.isEmpty()){
      mi.error("results not found");
    }
    // Create maps for quick lookup
        Map<String, Map<String, String>> fgledgMap = new HashMap<>();
        for (Map<String, String> g : fgledgList) {
            String key = g.get("CONO").toString() + "-" + g.get("DIVI").toString() + "-" + g.get("YEA4").toString() + "-" + g.get("JRNO").toString() + "-" + g.get("JSNO").toString();
            if(!fgledgMap.containsKey(key)){
              fgledgMap.put(key, g);
            }
        }

        Map<String, Map<String, String>> fdrfmaMap = new HashMap<>();
        for (Map<String, String> d : fdrfmaList) {
            String key = d.get("CONO").toString() + "-" + d.get("DIVI").toString() + "-" + d.get("CINO").toString();
            if(!fdrfmaMap.containsKey(key)){
              fdrfmaMap.put(key, d);
            }
        }
        
        Map<String, Map<String, String>> csynbvMap = new HashMap<>();
        for (Map<String, String> v : csynbvList) {
            String key = v.get("CONO").toString() + "-" + v.get("DIVI").toString() + "-" + v.get("VSER").toString();
            if(!csynbvMap.containsKey(key)){
              csynbvMap.put(key, v);
            }
        }
        
        Map<String, Map<String, String>> ocusmaMap = new HashMap<>();
        for (Map<String, String> o : ocusmaList) {
            String key = o.get("CONO").toString() + "-" + o.get("CUNO").toString();
            if(!ocusmaMap.containsKey(key)){
              ocusmaMap.put(key, o);
            }
        }

        List<Map<String, String>> resultList = new ArrayList<>();
         
        for (Map<String, String> s : fsledgList) {
            String keyG = s.get("CONO").toString() + "-" + s.get("DIVI").toString() + "-" + s.get("YEA4").toString() + "-" + s.get("JRNO").toString() + "-" + s.get("JSNO").toString();
            Map<String, String> g = fgledgMap.get(keyG);

            String keyD = s.get("CONO").toString() + "-" + s.get("DIVI").toString() + "-" + s.get("CINO").toString();
            Map<String, String> d = fdrfmaMap.get(keyD);
            
            String keyV = s.get("CONO").toString() + "-" + s.get("DIVI").toString() + "-" + s.get("VSER").toString();
            Map<String, String> v = csynbvMap.get(keyV);
            
            String keyO = s.get("CONO").toString() + "-" + s.get("CUNO").toString();
            Map<String, String> o = ocusmaMap.get(keyO);
            
            String keyP = s.get("CONO").toString() + "-" + s.get("PYNO").toString();
            Map<String, String> p = ocusmaMap.get(keyP);
            
            
            /*if(!isPayer&&isFromCust){
              condition=o.get("PYNO").equals("")?true:s.get("PYNO")==o.get("PYNO");
            }*/

            if (g != null&& v!=null ) {
            
              //Yael 05/11/2025
              String trcd = g.get("TRCD");
              String feid = g.get("FEID");
              String fncn = g.get("FNCN");
              String ivtp = s.get("IVTP");
              String pyrs = s.get("PYRS");
              //
             
              /*if((g.get("TRCD").equals("10") 
              && !s.get("IVTP").equals("AP")) 
              ||((g.get("TRCD").equals("10") 
              ||g.get("TRCD").equals("20"))
              &&g.get("FEID").equals("AB10")
              &&g.get("FNCN").equals("100")
              /*&&condition)){*/
              
              //WHERE condition
              if(
              ((trcd.equals("10") && !ivtp.equals("AP")) ||
              ((trcd.equals("10") || trcd.equals("20")) && feid.equals("AB10") && fncn.equals("100")) ||
              (feid.equals("AR20") || feid.equals("OI20")) ||
              //Yael 05/11/2025
              (trcd.equals("20") && (feid.equals("AP30") || feid.equals("AP32")) &&
              (pyrs == null || pyrs.isEmpty() || pyrs.compareTo("70") < 0)) ||
              (trcd.equals("20") && feid.startsWith("AP") && !fncn.equals("500")))
               ) {
                
                Map<String, String> result = new HashMap<>();
                result.put("CONO", s.get("CONO"));
                result.put("DIVI", s.get("DIVI"));
                result.put("PYNO", s.get("PYNO"));
                result.put("CUNO", s.get("CUNO"));
                result.put("TX15", v.get("TX15"));
                result.put("VONO", s.get("VONO"));
                result.put("ACDT", s.get("ACDT"));
                result.put("OCDT", g.get("OCDT"));
                
                if(feid.equals("AR20") || feid.equals("OI20")){
                  result.put("DBCR", g.get("DBCR"));//סכום בחובה או בזכות
                  result.put("DUDT", s.get("DUDT"));
                  result.put("CUAM", round(s.get("CUAM"), 2));
                  result.put("ACAM", round(g.get("ACAM"), 2));
                  result.put("TCAM", round(g.get("TCAM"), 2));
                }
                //סכום בזכות
                else if(feid.equals("AR30") && g.get("DBCR").equals("D")){
                  result.put("DBCR", "C");
                  result.put("DUDT", d.get("DUDT"));
                  result.put("CUAM", round(String.valueOf(Double.valueOf(s.get("CUAM"))*-1), 2));
                  result.put("ACAM", round(String.valueOf(Double.valueOf(g.get("ACAM"))*-1), 2));
                  result.put("TCAM", round(String.valueOf(Double.valueOf(g.get("TCAM"))*-1), 2));
                  
                }
                else{
                  result.put("DBCR", g.get("DBCR"));//סכום בחובה או בזכות
                  result.put("DUDT", d != null ? d.get("DUDT") : s.get("DUDT"));
                  result.put("CUAM", round(s.get("CUAM"), 2));
                  result.put("ACAM", round(g.get("ACAM"), 2));
                  result.put("TCAM", round(g.get("TCAM"), 2));
                }
                result.put("IVTP", ivtp);
                result.put("TRCD", trcd);
                result.put("CINO", s.get("CINO"));
                result.put("DRRN", d != null ? d.get("DRRN") : null);
                result.put("VTXT", g.get("VTXT"));
                result.put("ARAT", round(g.get("ARAT"), 6));
                result.put("TCAR", round(g.get("TCAR"), 6));
                result.put("PYRS", pyrs);
                result.put("FEID", feid);
                result.put("FNCN", fncn);
                result.put("REFE", d == null ? s.get("CINO") :  d.get("DRRN"));
                result.put("CUNM", o.get("CUNM"));
                result.put("CUCD", s.get("CUCD"));
                result.put("PYNM", p.get("CUNM"));
                result.put("VDSC", g.get("VDSC"));//Yael. 01/12/2025
                
                resultList.add(result);
              
        }
        //resultList.sort(Comparator.comparing(m -> (Comparable) m.get("ACDT")));
      }
      // else{
      //   mi.error("results not found");
      // }
      
    }
    printData(resultList);
    
  }
  
  
  void printData(List<Map<String, String>> resultList){
    BigDecimal sumTotalCUAM=BigDecimal.ZERO;
    BigDecimal sumTotalACAM=BigDecimal.ZERO;
    BigDecimal sumTotalTCAM=BigDecimal.ZERO;
        
    BigDecimal sumDCUAM=BigDecimal.ZERO;
    BigDecimal sumDACAM=BigDecimal.ZERO;
    BigDecimal sumDTCAM=BigDecimal.ZERO;
    BigDecimal sumCCUAM=BigDecimal.ZERO;
    BigDecimal sumCACAM=BigDecimal.ZERO;
    BigDecimal sumCTCAM=BigDecimal.ZERO;
      
    int cntLines=-1;
    boolean firstCustOpenLine=true;
    boolean hasOpenLine=false;
    boolean condition=true;
    String sumLineTotalCUAM="";
    String sumLineTotalACAM="";
    String sumLineTotalTCAM="";
    
    String LineDCUAM="";
    String LineDACAM="";
    String LineDTCAM="";
    String LineCCUAM="";
    String LineCACAM="";
    String LineCTCAM="";
    
    // if(resultList.isEmpty()){
    //   printOutLine(null,"י. פתיחה למשלם",sumDCUAM,"לתאריך",sumDACAM,sumDTCAM,sumCCUAM,sumCACAM,sumCTCAM,sumTotalCUAM,sumTotalACAM,sumTotalTCAM);
    //   return;
    // }
    String to="";
    if(isFromCust){
      to="ללקוח";
    }
    else{
      to="למשלם";
    }
    
    for(Map<String, String> result : resultList){
      cntLines++;
      hasOpenLine=false;
      
      if(cntLines==0){
        if(fromDate==null||fromDate.equals("")){
          printOutLine(result,"י. פתיחה "+to,sumDCUAM.toString(),"",sumDACAM.toString(),sumDTCAM.toString(),sumCCUAM.toString(),sumCACAM.toString(),sumCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
         
          hasOpenLine=true;
        }
        else if(Integer.parseInt(result.get("ACDT"))>=Integer.parseInt(fromDate)){
          printOutLine(result,"י. פתיחה "+to,sumDCUAM.toString(),"לתאריך "+fromDate,sumDACAM.toString(),sumDTCAM.toString(),sumCCUAM.toString(),sumCACAM.toString(),sumCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
          hasOpenLine=true;
        }
      }
      sumTotalCUAM=(sumTotalCUAM.add(new BigDecimal(result.get("CUAM"))));
      sumTotalACAM=(sumTotalACAM.add(new BigDecimal(result.get("ACAM"))));
      sumTotalTCAM=(sumTotalTCAM.add(new BigDecimal(result.get("TCAM"))));
                
      if(result.get("DBCR").equals("D")){
        sumDCUAM=(sumDCUAM.add(new BigDecimal(result.get("CUAM"))));
        sumDACAM=(sumDACAM.add(new BigDecimal(result.get("ACAM"))));
        sumDTCAM=(sumDTCAM.add(new BigDecimal(result.get("TCAM"))));
        LineDCUAM=result.get("CUAM");
        LineDACAM=result.get("ACAM");
        LineDTCAM=result.get("TCAM");
        LineCCUAM=BigDecimal.ZERO;
        LineCACAM=BigDecimal.ZERO;
        LineCTCAM=BigDecimal.ZERO;
      }
      else if(result.get("DBCR").equals("C")){
        sumCCUAM=(sumCCUAM.add(new BigDecimal(result.get("CUAM"))));
        sumCACAM=(sumCACAM.add(new BigDecimal(result.get("ACAM"))));
        sumCTCAM=(sumCTCAM.add(new BigDecimal(result.get("TCAM"))));
        LineDCUAM=BigDecimal.ZERO;
        LineDACAM=BigDecimal.ZERO;
        LineDTCAM=BigDecimal.ZERO;
        LineCCUAM=result.get("CUAM");
        LineCACAM=result.get("ACAM");
        LineCTCAM=result.get("TCAM");
      }
      
      if(resultList.size()==(cntLines+1)){
        if(fromDate!=null&&!fromDate.equals("")){
          if(Integer.parseInt(result.get("ACDT"))>=Integer.parseInt(fromDate)){
            printOutLine(result,"",LineDCUAM.toString(),"",LineDACAM.toString(),LineDTCAM.toString(),LineCCUAM.toString(),LineCACAM.toString(),LineCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
          }
          else{
            printOutLine(result,"י. פתיחה "+to,sumDCUAM.toString(),"לתאריך "+fromDate,sumDACAM.toString(),sumDTCAM.toString(),sumCCUAM.toString(),sumCACAM.toString(),sumCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
          }
        }
        else{
          printOutLine(result,"",LineDCUAM.toString(),"",LineDACAM.toString(),LineDTCAM.toString(),LineCCUAM.toString(),LineCACAM.toString(),LineCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
        }
        printOutLine(result,"י. סגירה "+to,sumDCUAM.toString(),"לתאריך "+toDate,sumDACAM.toString(),sumDTCAM.toString(),sumCCUAM.toString(),sumCACAM.toString(),sumCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
      }

      else if(fromDate!=null&&!fromDate.equals("")){
        if(Integer.parseInt(result.get("ACDT"))<Integer.parseInt(fromDate)&&Integer.parseInt(resultList.get(cntLines+1).get("ACDT"))>=Integer.parseInt(fromDate)){
          printOutLine(result,"י. פתיחה "+to,sumDCUAM.toString(),"לתאריך "+fromDate,sumDACAM.toString(),sumDTCAM.toString(),sumCCUAM.toString(),sumCACAM.toString(),sumCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
        }
        else if(Integer.parseInt(result.get("ACDT"))>=Integer.parseInt(fromDate)){
          printOutLine(result,"",LineDCUAM.toString(),"",LineDACAM.toString(),LineDTCAM.toString(),LineCCUAM.toString(),LineCACAM.toString(),LineCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
        }
      }
      else{
        printOutLine(result,"",LineDCUAM.toString(),"",LineDACAM.toString(),LineDTCAM.toString(),LineCCUAM.toString(),LineCACAM.toString(),LineCTCAM.toString(),sumTotalCUAM.toString(),sumTotalACAM.toString(),sumTotalTCAM.toString());
      }
    }
  }
  
   
  
   String round(String value, int places) {
        double val=Double.valueOf(value);
        BigDecimal bd = new BigDecimal(val).setScale(places, RoundingMode.HALF_UP);
        return bd.toString();
    }
    
    void sortTable(List<Map<String, String>> list,String field){
      
      for (int i = 0; i < list.size() - 1; i++) {
            for (int j = 0; j < list.size() - i - 1; j++) {
                Integer acdt1 = Integer.parseInt(list.get(j).get(field));
                Integer acdt2 =  Integer.parseInt( list.get(j + 1).get(field));

                if (acdt1 > acdt2) { // Swap if the first is greater
                    Map<String, String> temp = list.get(j);
                    list.set(j, list.get(j + 1));
                    list.set(j + 1, temp);
                }
            }
    }
    }
    
   
    void bubbleSort(List<Map<String, String>> list, String key1) {
        int n = list.size();
        boolean swapped;

        for (int i = 0; i < n - 1; i++) {
            swapped = false;

            for (int j = 0; j < n - i - 1; j++) {
                Map<String, String> map1 = list.get(j);
                Map<String, String> map2 = list.get(j + 1);

                String val1Key1 = map1.get(key1); // x as string
                String val2Key1 = map2.get(key1); // x as string

                // Compare x alphabetically
                if (val1Key1.compareTo(val2Key1) > 0) {
                    // Swap elements
                    Collections.swap(list, j, j + 1);
                    swapped = true;
                }
            }

            if (!swapped) break; // Optimization: Stop if no swaps
        }
    }
    
    String getCurrentDate(){
        Date currentDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String formattedDate = sdf.format(currentDate);
        return formattedDate;
    }
    
    void printOutLine(Map<String,String> curLine,String sumLineName,String sumDCUAM,String sumLineDesc,String sumDACAM,String sumDTCAM,String sumCCUAM
    ,String sumCACAM,String sumCTCAM,String sumTotalCUAM,String sumTotalACAM,String sumTotalTCAM){
      mi.outData.put("CONO", curLine.get("CONO"));
      mi.outData.put("DIVI", curLine.get("DIVI"));
      mi.outData.put("PYNO", isPayer&&!isFromCust&&!sumLineName.equals("")?curLine.get("PYNO"):"");
      //mi.outData.put("CUNO", isFromCust||!sumLineName.equals("")?"":curLine.get("CUNO"));
      mi.outData.put("CUNO", isPayer&&!isFromCust&&sumLineName.equals("")||isFromCust&&!sumLineName.equals("")?curLine.get("CUNO"):"");
      mi.outData.put("TX15", (sumLineName.equals("")?curLine.get("TX15"):sumLineName));
      mi.outData.put("VONO", (sumLineName.equals("")?curLine.get("VONO"):""));
      mi.outData.put("ACDT", (sumLineName.equals("")?curLine.get("ACDT"):""));
      mi.outData.put("OCDT", (sumLineName.equals("")?curLine.get("OCDT"):""));
      mi.outData.put("DBCR", (sumLineName.equals("")?curLine.get("DBCR"):""));
      mi.outData.put("DUDT", (sumLineName.equals("")?curLine.get("DUDT"):""));
      mi.outData.put("DCUA", round(sumDCUAM.toString(),2));
      mi.outData.put("DACA", round(sumDACAM.toString(),2));
      mi.outData.put("DTCA", round(sumDTCAM.toString(),2));
      mi.outData.put("CCUA", round(sumCCUAM.toString(),2));
      mi.outData.put("CACA", round(sumCACAM.toString(),2));
      mi.outData.put("CTCA", round(sumCTCAM.toString(),2));
      mi.outData.put("ARAT", (sumLineName.equals("")?curLine.get("ARAT"):""));
      mi.outData.put("TCAR", (sumLineName.equals("")?curLine.get("TCAR"):""));
      mi.outData.put("PYRS", (sumLineName.equals("")?curLine.get("PYRS"):""));
      mi.outData.put("IVTP", (sumLineName.equals("")?curLine.get("IVTP"):""));
      mi.outData.put("TRCD", (sumLineName.equals("")?curLine.get("TRCD"):""));
      mi.outData.put("FEID", (sumLineName.equals("")?curLine.get("FEID"):""));
      mi.outData.put("FNCN", (sumLineName.equals("")?curLine.get("FNCN"):""));
      mi.outData.put("REFE", (sumLineName.equals("")?curLine.get("REFE"):""));
      // mi.outData.put("VTXT", sumLineDesc);
      mi.outData.put("VTXT", curLine.get("VTXT") == null ?  "" :  curLine.get("VTXT"));
      mi.outData.put("TCUA", round(sumTotalCUAM.toString(),2));
      mi.outData.put("TACA", round(sumTotalACAM.toString(),2));
      mi.outData.put("TTCA", round(sumTotalTCAM.toString(),2));
      //mi.outData.put("CUNM", isFromCust||!sumLineName.equals("")?"":curLine.get("CUNM"));
      mi.outData.put("CUNM",isPayer&&!isFromCust&&sumLineName.equals("")||isFromCust&&!sumLineName.equals("")?curLine.get("CUNM"):"");
      mi.outData.put("CUCD", curLine.get("CUCD"));
      // mi.outData.put("PYNM", isPayer&&!isFromCust&&!sumLineName.equals("")?curLine.get("PYNM"):"");
      mi.outData.put("PYNM", isPayer&&!isFromCust&&!sumLineName.equals("")?(curLine.get("PYNM") != null ? curLine.get("PYNM") : ""):"");
      mi.outData.put("VDSC", curLine.get("VDSC"));//Yael. 01/10/2025
      mi.write();
    }
    
  
  void readRecordsFSLEDG() {
    int nrOfKeys = 0;
    //int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords(); 
    int nrOfRecords=9999;
    ExpressionFactory expressionFSLEDG = database.getExpressionFactory("FSLEDG");
    /*if(isFromCust&&isToCust){
       expressionFSLEDG=expressionFSLEDG.ge("ESPYNO",fromCustomer).and(expressionFSLEDG.le("ESPYNO",toCustomer));
    }
    else{
      String cust=isToCust ? toCustomer : fromCustomer;
      expressionFSLEDG=expressionFSLEDG.eq("ESPYNO",cust);
    }*/
    if(isPayer){
      if(!isFromCust){
         expressionFSLEDG=expressionFSLEDG.eq("ESPYNO",payer);
         expressionFSLEDG=expressionFSLEDG.or(expressionFSLEDG.eq("ESCUNO",payer))
      }
      else{
         expressionFSLEDG=expressionFSLEDG.eq("ESPYNO",payer);
         expressionFSLEDG=expressionFSLEDG.and(expressionFSLEDG.eq("ESCUNO",fromCustomer))
      }
    }
    else{
      if(isFromCust&&isToCust){
        expressionFSLEDG=expressionFSLEDG.ge("ESCUNO",fromCustomer).and(expressionFSLEDG.le("ESCUNO",toCustomer));
      }
      else if(isFromCust){
        expressionFSLEDG=expressionFSLEDG.eq("ESCUNO",fromCustomer);
      }
      else if(isToCust)
       expressionFSLEDG=expressionFSLEDG.eq("ESCUNO",toCustomer);
    }
    //expressionFSLEDG=expressionFSLEDG.eq("ESPYNO",payer);
    //expressionFSLEDG=expressionFSLEDG.and(expressionFSLEDG.ge("ESCUNO",fromCustomer).and(expressionFSLEDG.le("ESCUNO",toCustomer)));
    //expressionFSLEDG = expressionFSLEDG.and(expressionFSLEDG.ne("ESIVTP","AP"));
    expressionFSLEDG=expressionFSLEDG.and(expressionFSLEDG.eq("ESPYRS","").or(expressionFSLEDG.lt("ESPYRS","70")));
    DBAction queryFSLEDG = database.table("FSLEDG")
      .index("00")
      .matching(expressionFSLEDG)
      .selection("ESCONO", "ESDIVI", "ESPYNO", "ESCUNO", "ESVSER","ESVONO","ESACDT","ESCUAM","ESDUDT","ESIVTP","ESCINO","ESPYRS","ESYEA4","ESJRNO","ESJSNO","ESCUCD")
      .build();
    DBContainer containerFSLEDG = queryFSLEDG.getContainer();
    queryFSLEDG.readAll(containerFSLEDG, nrOfKeys, nrOfRecords, callbackFSLEDG)
    
  }
  
  void readRecordsFGLEDG() {
    int nrOfKeys = 0;
    //int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords(); 
    int nrOfRecords=9999;
    ExpressionFactory expressionFGLEDG = database.getExpressionFactory("FGLEDG");
    expressionFGLEDG = expressionFGLEDG.eq("EGTRCD","10").or(expressionFGLEDG.eq("EGTRCD","20"));
    //expressionFGLEDG=expressionFGLEDG.and(expressionFGLEDG.ge("EGACDT",fromDate)).and(expressionFGLEDG.le("EGACDT",toDate));
    expressionFGLEDG=expressionFGLEDG.and(expressionFGLEDG.le("EGACDT",toDate));
    DBAction queryFGLEDG = database.table("FGLEDG")
      .index("00")
      .matching(expressionFGLEDG)
      .selection("EGOCDT", "EGTRCD", "EGVTXT", "EGACAM", "EGARAT","EGTCAM","EGTCAR","EGCONO","EGDIVI","EGYEA4","EGJRNO","EGJSNO","EGFNCN","EGFEID","EGDBCR", "EGVDSC")
      .build();
    DBContainer containerFGLEDG = queryFGLEDG.getContainer();
    queryFGLEDG.readAll(containerFGLEDG, nrOfKeys, nrOfRecords, callbackFGLEDG)
    
  }
  
  void readRecordsFDRFMA() {
    int nrOfKeys = 0;
    //int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords();
    int nrOfRecords=9999;
    DBAction queryFDRFMA = database.table("FDRFMA")
      .index("00")
      .selection("DMDRRN", "DMDUDT", "DMCONO", "DMDIVI", "DMCINO")
      .build();
    DBContainer containerFDRFMA = queryFDRFMA.getContainer();
    queryFDRFMA.readAll(containerFDRFMA, nrOfKeys, nrOfRecords, callbackFDRFMA)
  }
  
  void readRecordsCSYNBV() {
    int nrOfKeys = 0;
    //int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords();
    int nrOfRecords=9999;
    DBAction queryCSYNBV = database.table("CSYNBV")
      .index("00")
      .selection("DVTX15","DVCONO","DVDIVI","DVVSER")
      .build();
    DBContainer containerCSYNBV = queryCSYNBV.getContainer();
    queryCSYNBV.readAll(containerCSYNBV, nrOfKeys, nrOfRecords, callbackCSYNBV)
  }
  
  void readRecordsOCUSMA(String cono, String cuno) {
    int nrOfKeys = 0;
    //int nrOfRecords = mi.getMaxRecords() <= 0 || mi.getMaxRecords() >= 10000? 10000: mi.getMaxRecords();
    int nrOfRecords=9999;
    ExpressionFactory expressionOCUSMA = database.getExpressionFactory("OCUSMA");
    expressionOCUSMA = expressionOCUSMA.eq("OKCONO",cono);
    expressionOCUSMA = expressionOCUSMA.and(expressionOCUSMA.eq("OKCUNO",cuno));
    DBAction queryOCUSMA = database.table("OCUSMA")
      .index("00")
      .matching(expressionOCUSMA)
      .selection("OKCUNO","OKCUNM","OKCONO")
      .build();
    DBContainer containerOCUSMA = queryOCUSMA.getContainer();
    queryOCUSMA.readAll(containerOCUSMA, nrOfKeys, nrOfRecords, callbackOCUSMA);
  }
  
  Closure<?> callbackOCUSMA = { DBContainer container ->
    Map<String, String> curRow=new HashMap<>();
    curRow.put("CUNO",container.get("OKCUNO")==null?"":container.get("OKCUNO").toString().trim());
    curRow.put("CUNM",container.get("OKCUNM")==null?"":container.get("OKCUNM").toString().trim());
    curRow.put("CONO",container.get("OKCONO")==null?"":container.get("OKCONO").toString().trim());
    curRow.put("PYNO",container.get("OKPYNO")==null?"":container.get("OKPYNO").toString().trim());
    ocusmaList.add(curRow);
    
  }
  
  Closure<?> callbackFSLEDG = { DBContainer container ->
    Map<String, String> curRow=new HashMap<>();
    curRow.put("CONO",container.get("ESCONO")==null?"":container.get("ESCONO").toString().trim());
    curRow.put("DIVI",container.get("ESDIVI")==null?"":container.get("ESDIVI").toString().trim());
    curRow.put("PYNO",container.get("ESPYNO")==null?"":container.get("ESPYNO").toString().trim());
    curRow.put("CUNO",container.get("ESCUNO")==null?"":container.get("ESCUNO").toString().trim());
    curRow.put("VSER",container.get("ESVSER")==null?"":container.get("ESVSER").toString().trim());
    curRow.put("VONO",container.get("ESVONO")==null?"":container.get("ESVONO").toString().trim());
    curRow.put("ACDT",container.get("ESACDT")==null?"":container.get("ESACDT").toString().trim());
    curRow.put("CUAM",container.get("ESCUAM")==null?"":container.get("ESCUAM").toString().trim());
    curRow.put("DUDT",container.get("ESDUDT")==null?"":container.get("ESDUDT").toString().trim());
    curRow.put("IVTP",container.get("ESIVTP")==null?"":container.get("ESIVTP").toString().trim());
    curRow.put("CINO",container.get("ESCINO")==null?"":container.get("ESCINO").toString().trim());
    curRow.put("PYRS",container.get("ESPYRS")==null?"":container.get("ESPYRS").toString().trim());
    curRow.put("YEA4",container.get("ESYEA4")==null?"":container.get("ESYEA4").toString().trim());
    curRow.put("JRNO",container.get("ESJRNO")==null?"":container.get("ESJRNO").toString().trim());
    curRow.put("JSNO",container.get("ESJSNO")==null?"":container.get("ESJSNO").toString().trim());
    curRow.put("CUCD",container.get("ESCUCD")==null?"":container.get("ESCUCD").toString().trim());
    fsledgList.add(curRow);
    readRecordsOCUSMA(curRow.get("CONO"),curRow.get("CUNO"));
    readRecordsOCUSMA(curRow.get("CONO"),curRow.get("PYNO"));
    
  }
  
  Closure<?> callbackFGLEDG = { DBContainer container ->
    Map<String, String> curRow=new HashMap<>();
    curRow.put("OCDT",container.get("EGOCDT")==null?"":container.get("EGOCDT").toString().trim());
    curRow.put("TRCD",container.get("EGTRCD")==null?"":container.get("EGTRCD").toString().trim());
    curRow.put("VTXT",container.get("EGVTXT")==null?"":container.get("EGVTXT").toString().trim());
    curRow.put("ACAM",container.get("EGACAM")==null?"":container.get("EGACAM").toString().trim());
    curRow.put("ARAT",container.get("EGARAT")==null?"":container.get("EGARAT").toString().trim());
    curRow.put("TCAM",container.get("EGTCAM")==null?"":container.get("EGTCAM").toString().trim());
    curRow.put("TCAR",container.get("EGTCAR")==null?"":container.get("EGTCAR").toString().trim());
    curRow.put("CONO",container.get("EGCONO")==null?"":container.get("EGCONO").toString().trim());
    curRow.put("DIVI",container.get("EGDIVI")==null?"":container.get("EGDIVI").toString().trim());
    curRow.put("YEA4",container.get("EGYEA4")==null?"":container.get("EGYEA4").toString().trim());
    curRow.put("JRNO",container.get("EGJRNO")==null?"":container.get("EGJRNO").toString().trim());
    curRow.put("JSNO",container.get("EGJSNO")==null?"":container.get("EGJSNO").toString().trim());
    curRow.put("FNCN",container.get("EGFNCN")==null?"":container.get("EGFNCN").toString().trim());
    curRow.put("FEID",container.get("EGFEID")==null?"":container.get("EGFEID").toString().trim());
    curRow.put("DBCR",container.get("EGDBCR")==null?"":container.get("EGDBCR").toString().trim());
    curRow.put("VDSC",container.get("EGVDSC")==null?"":container.get("EGVDSC").toString().trim());//Yael. 01/12/2025
    fgledgList.add(curRow);
  }
  
  Closure<?> callbackFDRFMA = { DBContainer container ->
    Map<String, String> curRow=new HashMap<>();
    curRow.put("DRRN",container.get("DMDRRN")==null?"":container.get("DMDRRN").toString().trim());
    curRow.put("DUDT",container.get("DMDUDT")==null?"":container.get("DMDUDT").toString().trim());
    curRow.put("CONO",container.get("DMCONO")==null?"":container.get("DMCONO").toString().trim());
    curRow.put("DIVI",container.get("DMDIVI")==null?"":container.get("DMDIVI").toString().trim());
    curRow.put("CINO",container.get("DMCINO")==null?"":container.get("DMCINO").toString().trim());
    fdrfmaList.add(curRow);
    
  }
  
  Closure<?> callbackCSYNBV = { DBContainer container ->
    Map<String, String> curRow=new HashMap<>();
    curRow.put("TX15",container.get("DVTX15")==null?"":container.get("DVTX15").toString().trim());
    curRow.put("CONO",container.get("DVCONO")==null?"":container.get("DVCONO").toString().trim());
    curRow.put("DIVI",container.get("DVDIVI")==null?"":container.get("DVDIVI").toString().trim());
    curRow.put("VSER",container.get("DVVSER")==null?"":container.get("DVVSER").toString().trim());
    csynbvList.add(curRow);
    
  }
  
  /*List<Map<String, Integer>> mergeMaps(List<String> joinKeys, List<List<Map<String, Integer>>> lists) {
        Map<String, Map<String, Integer>> resultMap = new HashMap<>();
        for (List<Map<String, Integer>> list : lists) {
            for (Map<String, Integer> row : list) {
                StringBuilder keyBuilder = new StringBuilder();
                for (String joinKey : joinKeys) {
                    keyBuilder.append(row.get(joinKey)).append("-");
                }
                String key = keyBuilder.toString();
                key = key.substring(0, key.length() - 1);
                if (!resultMap.containsKey(key)) {
                    resultMap.put(key, new HashMap<String, Integer>());
                    for (String joinKey : joinKeys) {
                        resultMap.get(key).put(joinKey, row.get(joinKey));
                    }
                }

                for (Map.Entry<String, Integer> entry : row.entrySet()) {
                    if (!joinKeys.contains(entry.getKey())) {
                        resultMap.get(key).put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }

        return new ArrayList<>(resultMap.values());
    }*/
}