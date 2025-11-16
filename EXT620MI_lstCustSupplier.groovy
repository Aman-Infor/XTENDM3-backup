import java.util.*

public class lstCustSupplier extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  // ---------------- Columns (as constants) ----------------
  private static final String[] FPLEDG_FIELDS = [
    "EPCONO","EPDIVI","EPYEA4","EPJRNO","EPJSNO","EPACDT",
    "EPSUNO","EPSINO","EPIVDT","EPVSER","EPVONO","EPDUDT","EPCUCD",
    "EPCUAM","EPTRCD","EPIVTP"
  ]
  private static final String[] FGLEDG_FIELDS = [
    "EGCONO","EGDIVI","EGYEA4","EGJRNO","EGJSNO","EGFEID","EGFNCN","EGVTXT","EGACAM"
  ]
  private static final String[] FAPCHK_FIELDS_SAFE =  [
    "CKCONO","CKDIVI","CKVSER","CKVONO","CKCHKN"
  ]
  private static final String[] CSYNBV_FIELDS = [
    "DVCONO","DVDIVI","DVVSER","DVTX15"
  ]
  private static final String[] FPLEDX_FIELDS = [
    "EPCONO","EPDIVI","EPYEA4","EPJRNO","EPJSNO","EPPEXN","EPPEXI"
  ]
  private static final String[] FSLEDG_FIELDS = [
    "ESCONO","ESDIVI","ESYEA4","ESJRNO","ESJSNO","ESACDT",
    "ESPYNO","ESCINO","ESIVDT","ESVSER","ESVONO","ESDUDT","ESCUCD",
    "ESCUAM","ESIVTP","ESTRCD","ESPYRS"
  ]
  private static final String[] FSLEDX_FIELDS = [
    "SECONO","SEDIVI","SEYEA4","SEJRNO","SEJSNO","SEXN","SEXI"
  ]
  private static final String[] OCUSMA_FIELDS = [
    "OKCONO","OKCUNO","OKCUNM"
  ]
  private static final String[] CIDMAS_FIELDS = [
    "IDCONO","IDSUNO","IDSUNM"
  ]

  // ---------------- Inputs ----------------
  private int    CONO
  private String DIVI, SUNO, SCNO, CUNO, PYNO
  private int    FDAT, TDAT

  // running balance accumulators
  private double opening = 0d
  private double running = 0d
  
  private List<Map<String,String>> allRows = []

  public lstCustSupplier(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  }

  public void main() {
    // Parse inputs
    CONO = safeInt2(mi.inData.get("CONO"), program.getLDAZD().CONO)
    DIVI = s(mi.inData.get("DIVI"))
    SUNO = s(mi.inData.get("SUNO"))
    PYNO = s(mi.inData.get("PYNO"))
    SCNO = s(mi.inData.get("SCNO"))
    CUNO = s(mi.inData.get("CUNO"))
    FDAT = toInt(mi.inData.get("FDAT"))
    TDAT = toInt(mi.inData.get("TDAT"))

    if (isEmpty(DIVI)) { mi.error("DIVI is required"); return }
    if (CONO <= 0)     { mi.error("CONO is required"); return }
    if (TDAT != 0 && FDAT != 0 && TDAT < FDAT) { mi.error("TDAT must be >= FDAT"); return }

    // Opening balance (до FDAT) = AP + AR (в валюте движения, т.е. CUAM)
    opening = sumOpeningAP(CONO, DIVI, SUNO, FDAT) + sumOpeningAR(CONO, DIVI, firstNZ3(SCNO, CUNO, PYNO), FDAT)
    running = opening

    // AP branch (supplier): можно не вызывать, если не выбран SUNO
    if (!isEmpty(SUNO)) {
      readFPLEDG()
    }

    // AR branch (linked customer). По ТЗ сообщаем об отсутствии связанного клиента.
    String linked = firstNZ3(SCNO, CUNO, PYNO)
    if (isEmpty(linked)) {
      mi.error("No linked customer (SCNO/CUNO/PYNO is empty) for the selected supplier")
      return
    }
    readFSLEDG(linked)
    sendToOutput();
  }

  // --------------------------------------------------------
  // Opening sums
  // --------------------------------------------------------
  private double sumOpeningAP(int cono, String divi, String suno, int beforeAcdt) {
    if (isEmpty(suno) || beforeAcdt == 0) return 0d
    ExpressionFactory ef = database.getExpressionFactory("FPLEDG")
    ExpressionFactory exp = ef.eq("EPCONO", String.valueOf(cono))
      .and(ef.eq("EPDIVI", divi))
      .and(ef.eq("EPSUNO", suno))
      .and(ef.lt("EPACDT", String.valueOf(beforeAcdt)))
      // те же бизнес-ограничения, что и в основной выборке
      .and( ef.eq("EPIVTP","AP").or( ef.eq("EPTRCD","40").or( ef.eq("EPTRCD","50") ) ) )

    DBAction a = database.table("FPLEDG").index("00").matching(exp)
      .selection("EPCUAM")
      .build()

    DBContainer c = a.getContainer()
    final double[] sum = new double[1]
    a.readAll(c, 0, 100000, { DBContainer row ->
      sum[0] += toDouble(row.get("EPCUAM"))
    })
    return sum[0]
  }

  private double sumOpeningAR(int cono, String divi, String payer, int beforeAcdt) {
    if (isEmpty(payer) || beforeAcdt == 0) return 0d
    ExpressionFactory ef = database.getExpressionFactory("FSLEDG")
    ExpressionFactory exp = ef.eq("ESCONO", String.valueOf(cono))
      .and(ef.eq("ESDIVI", divi))
      .and(ef.eq("ESPYNO", payer))
      .and(ef.lt("ESACDT", String.valueOf(beforeAcdt)))
      // мягкий фильтр по ТЗ
      .and( ef.eq("ESPYRS","").or( ef.lt("ESPYRS","70") ) )

    DBAction a = database.table("FSLEDG").index("00").matching(exp)
      .selection("ESCUAM")
      .build()

    DBContainer c = a.getContainer()
    final double[] sum = new double[1]
    a.readAll(c, 0, 100000, { DBContainer row ->
      sum[0] += toDouble(row.get("ESCUAM"))
    })
    return sum[0]
  }

  // =================================================================
  // AP: FPLEDG + FGLEDG (+ FAPCHK / CSYNBV / FPLEDX / CIDMAS)
  // =================================================================
  void readFPLEDG() {
    ExpressionFactory ef = database.getExpressionFactory("FPLEDG")
    ExpressionFactory exp = ef.eq("EPCONO", String.valueOf(CONO))
      .and(ef.eq("EPDIVI", DIVI))
      .and(ef.eq("EPSUNO", SUNO))

    if (FDAT != 0) exp = exp.and(ef.ge("EPACDT", String.valueOf(FDAT)))
    if (TDAT != 0) exp = exp.and(ef.le("EPACDT", String.valueOf(TDAT)))

    // (EPIVTP='AP') OR (EPTRCD in 40,50)
    exp = exp.and( ef.eq("EPIVTP","AP").or( ef.eq("EPTRCD","40").or( ef.eq("EPTRCD","50") ) ) )

    DBAction action = database.table("FPLEDG").index("00").matching(exp)
      .selection(FPLEDG_FIELDS)
      .build()
    DBContainer c = action.getContainer()

    action.readAll(c, 0, 100000, { DBContainer fp ->
      Map<String,String> r = new HashMap<String,String>()
      for (String f : FPLEDG_FIELDS) {
        Object v = fp.get(f)
        r.put(f, v == null ? "" : v.toString())
      }

      joinFGLEDG_fromFP(r)
      joinFAPCHK(r)
      joinCSYNBV_fromVSER_AP(r)
      joinFPLEDX(r)
      joinCIDMAS(r, r.get("EPSUNO"))  // имя поставщика

      writeRow(r, false) // AP
    })
  }

  // FGLEDG join for AP
  void joinFGLEDG_fromFP(Map<String,String> r) {
    ExpressionFactory ef = database.getExpressionFactory("FGLEDG")
    ExpressionFactory exp = ef.eq("EGCONO", r.get("EPCONO"))
      .and(ef.eq("EGDIVI", r.get("EPDIVI")))
      .and(ef.eq("EGYEA4", r.get("EPYEA4")))
      .and(ef.eq("EGJRNO", r.get("EPJRNO")))
      .and(ef.eq("EGJSNO", r.get("EPJSNO")))
      // по заметке: EGFEID='AP10' OR EGFNCN!='500'
      .and( ef.eq("EGFEID","AP10").or( ef.ne("EGFNCN","500") ) )

    DBAction a = database.table("FGLEDG").index("00").matching(exp)
      .selection(FGLEDG_FIELDS)
      .build()
    DBContainer c = a.getContainer()

    a.readAll(c, 0, 5, { DBContainer fg ->
      for (String f : FGLEDG_FIELDS) {
        Object v = fg.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // FAPCHK (LF 00) for AP
  void joinFAPCHK(Map<String,String> r) {
    String vser = r.get("EPVSER")
    String vono = r.get("EPVONO")
    if (isEmpty(vser) || isEmpty(vono)) return

    ExpressionFactory ef = database.getExpressionFactory("FAPCHK")
    ExpressionFactory exp = ef.eq("CKCONO", r.get("EPCONO"))
      .and(ef.eq("CKDIVI", r.get("EPDIVI")))
      .and(ef.eq("CKVSER", vser))
      .and(ef.eq("CKVONO", vono))

    DBAction a = database.table("FAPCHK").index("00").matching(exp)
      .selection(FAPCHK_FIELDS_SAFE)
      .build()
    DBContainer c = a.getContainer()

    a.readAll(c, 0, 1, { DBContainer ck ->
      for (String f : FAPCHK_FIELDS_SAFE) {
        Object v = ck.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // CSYNBV short text for AP by VSER
  void joinCSYNBV_fromVSER_AP(Map<String,String> r) {
    String vser = r.get("EPVSER")
    if (isEmpty(vser)) return

    ExpressionFactory ef = database.getExpressionFactory("CSYNBV")
    ExpressionFactory exp = ef.eq("DVCONO", r.get("EPCONO"))
      .and(ef.eq("DVDIVI", r.get("EPDIVI")))
      .and(ef.eq("DVVSER", vser))

    DBAction action = database.table("CSYNBV").index("00").matching(exp)
      .selection(CSYNBV_FIELDS)
      .build()
    DBContainer c = action.getContainer()

    action.readAll(c, 0, 1, { DBContainer cs ->
      for (String f : CSYNBV_FIELDS) {
        Object v = cs.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // FPLEDX (EPPEXN=409 -> MASAV/PROP number)
  void joinFPLEDX(Map<String,String> r) {
    ExpressionFactory ef = database.getExpressionFactory("FPLEDX")
    ExpressionFactory exp = ef.eq("EPCONO", r.get("EPCONO"))
      .and(ef.eq("EPDIVI", r.get("EPDIVI")))
      .and(ef.eq("EPYEA4", r.get("EPYEA4")))
      .and(ef.eq("EPJRNO", r.get("EPJRNO")))
      .and(ef.eq("EPJSNO", r.get("EPJSNO")))
      .and(ef.eq("EPPEXN", "409"))

    DBAction a = database.table("FPLEDX").index("00").matching(exp)
      .selection(FPLEDX_FIELDS)
      .build()
    DBContainer c = a.getContainer()

    a.readAll(c, 0, 3, { DBContainer dx ->
      for (String f : FPLEDX_FIELDS) {
        Object v = dx.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // CIDMAS join for supplier name
  void joinCIDMAS(Map<String,String> r, String supplierNumber) {
    if (isEmpty(supplierNumber)) return
    
    ExpressionFactory ef = database.getExpressionFactory("CIDMAS")
    ExpressionFactory exp = ef.eq("IDCONO", String.valueOf(CONO))
      .and(ef.eq("IDSUNO", supplierNumber))
    
    DBAction a = database.table("CIDMAS").index("00").matching(exp)
      .selection(CIDMAS_FIELDS)
      .build()
    DBContainer c = a.getContainer()
    
    a.readAll(c, 0, 1, { DBContainer cd ->
      for (String f : CIDMAS_FIELDS) {
        Object v = cd.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // =================================================================
  // AR: FSLEDG + FGLEDG (+ CSYNBV + FSLEDX[212,213,214,215] + OCUSMA)
  // =================================================================
  void readFSLEDG(String linkedCustomer) {
    ExpressionFactory ef = database.getExpressionFactory("FSLEDG")
    ExpressionFactory exp = ef.eq("ESCONO", String.valueOf(CONO))
      .and(ef.eq("ESDIVI", DIVI))
      .and(ef.eq("ESPYNO", linkedCustomer))

    if (FDAT != 0) exp = exp.and(ef.ge("ESACDT", String.valueOf(FDAT)))
    if (TDAT != 0) exp = exp.and(ef.le("ESACDT", String.valueOf(TDAT)))

    // мягкий фильтр
    exp = exp.and( ef.eq("ESPYRS","").or( ef.lt("ESPYRS","70") ) )

    DBAction action = database.table("FSLEDG").index("00").matching(exp)
      .selection(FSLEDG_FIELDS)
      .build()
    DBContainer c = action.getContainer()

    action.readAll(c, 0, 100000, { DBContainer fs ->
      Map<String,String> r = new HashMap<String,String>()
      for (String f : FSLEDG_FIELDS) {
        Object v = fs.get(f)
        r.put(f, v == null ? "" : v.toString())
      }

      joinFGLEDG_fromFS(r)
      joinCSYNBV_fromVSER_AR(r)
      joinOCUSMA(r, r.get("ESPYNO"))  // имя клиента
      // joinFSLEDX_one(r, "213", "CUST_CK")
      // joinFSLEDX_one(r, "214", "C_S_CK")
      // joinFSLEDX_one(r, "215", "BANK_PAGE")
      // joinFSLEDX_one(r, "212", "PROP_N")

      writeRow(r, true) // AR
    })
  }

  // FGLEDG join for AR
  void joinFGLEDG_fromFS(Map<String,String> r) {
    ExpressionFactory ef = database.getExpressionFactory("FGLEDG")
    ExpressionFactory exp = ef.eq("EGCONO", r.get("ESCONO"))
      .and(ef.eq("EGDIVI", r.get("ESDIVI")))
      .and(ef.eq("EGYEA4", r.get("ESYEA4")))
      .and(ef.eq("EGJRNO", r.get("ESJRNO")))
      .and(ef.eq("EGJSNO", r.get("ESJSNO")))

    DBAction a = database.table("FGLEDG").index("00").matching(exp)
      .selection(FGLEDG_FIELDS)
      .build()
    DBContainer c = a.getContainer()

    a.readAll(c, 0, 5, { DBContainer fg ->
      for (String f : FGLEDG_FIELDS) {
        Object v = fg.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // CSYNBV for AR by ESVSER
  void joinCSYNBV_fromVSER_AR(Map<String,String> r) {
    String vser = r.get("ESVSER")
    if (isEmpty(vser)) return

    String dvcono = isEmpty(r.get("ESCONO")) ? String.valueOf(CONO) : r.get("ESCONO")

    ExpressionFactory ef = database.getExpressionFactory("CSYNBV")
    ExpressionFactory exp = ef.eq("DVCONO", dvcono)
      .and(ef.eq("DVDIVI", r.get("ESDIVI")))
      .and(ef.eq("DVVSER", vser))

    DBAction action = database.table("CSYNBV").index("00").matching(exp)
      .selection(CSYNBV_FIELDS)
      .build()
    DBContainer c = action.getContainer()

    action.readAll(c, 0, 1, { DBContainer cs ->
      for (String f : CSYNBV_FIELDS) {
        Object v = cs.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // OCUSMA join for customer name
  void joinOCUSMA(Map<String,String> r, String customerNumber) {
    if (isEmpty(customerNumber)) return
    
    ExpressionFactory ef = database.getExpressionFactory("OCUSMA")
    ExpressionFactory exp = ef.eq("OKCONO", String.valueOf(CONO))
      .and(ef.eq("OKCUNO", customerNumber))
    
    DBAction a = database.table("OCUSMA").index("00").matching(exp)
      .selection(OCUSMA_FIELDS)
      .build()
    DBContainer c = a.getContainer()
    
    a.readAll(c, 0, 1, { DBContainer oc ->
      for (String f : OCUSMA_FIELDS) {
        Object v = oc.get(f)
        r.put(f, v == null ? "" : v.toString())
      }
    })
  }

  // FSLEDX reader for one SEXN -> write into outKey
  void joinFSLEDX_one(Map<String,String> r, String sexn, String outKey) {
    String secono = isEmpty(r.get("ESCONO")) ? String.valueOf(CONO) : r.get("ESCONO")

    ExpressionFactory ef = database.getExpressionFactory("FSLEDX")
    ExpressionFactory exp = ef.eq("SECONO", secono)
      .and(ef.eq("SEDIVI", r.get("ESDIVI")))
      .and(ef.eq("SEYEA4", r.get("ESYEA4")))
      .and(ef.eq("SEJRNO", r.get("ESJRNO")))
      .and(ef.eq("SEJSNO", r.get("ESJSNO")))
      .and(ef.eq("SEXN", sexn))

    DBAction action = database.table("FSLEDX").index("00").matching(exp)
      // ВАЖНО: выбираем только значение, никаких ключей
      .selection("SEXI")
      .build()
    DBContainer c = action.getContainer()

    action.readAll(c, 0, 1, { DBContainer dx ->
      Object v = dx.get("SEXI")
      String sVal = (v == null) ? "" : v.toString()
      if (!isEmpty(sVal)) r.put(outKey, sVal)
    })
  }

  // =================================================================
  // Unified output + running balance
  // =================================================================
 void writeRow(Map<String,String> r, boolean isAR) {
    Map<String,String> outputRow = [:]
    
    // Voucher identification
    String vser = isAR ? r.get("ESVSER") : r.get("EPVSER")
    String vono = isAR ? r.get("ESVONO") : r.get("EPVONO")
    outputRow.put("VSER", nz(vser))
    outputRow.put("VONO", nz(vono))
    outputRow.put("SIDE", isAR ? "Customer" : "Supplier")
    
    // Dates
    String acdt = isAR ? r.get("ESACDT") : r.get("EPACDT")
    outputRow.put("ACDT", nz(acdt))
    outputRow.put("DUDT", nz(isAR ? r.get("ESDUDT") : r.get("EPDUDT")))
    outputRow.put("IVDT", nz(isAR ? r.get("ESIVDT") : r.get("EPIVDT")))
    
    // Currency / amounts
    String cuamStr = isAR ? r.get("ESCUAM") : r.get("EPCUAM")
    double cuam = toDouble(cuamStr)
    outputRow.put("CUCD", nz(isAR ? r.get("ESCUCD") : r.get("EPCUCD")))
    outputRow.put("CUAM", nz(cuamStr))
    outputRow.put("LCAM", nz(r.get("EGACAM")))
    
    // Short/long text
    outputRow.put("TX15", nz(r.get("DVTX15")))
    outputRow.put("VTXT", nz(r.get("EGVTXT")))
    
    // Checks / bank / proposal
    outputRow.put("CHKN", nz(r.get("CKCHKN")))
    outputRow.put("CCHK", nz(r.get("CUST_CK")))
    outputRow.put("SCCK", nz(r.get("C_S_CK")))
    outputRow.put("BNKP", nz(r.get("BANK_PAGE")))
    outputRow.put("PRPN", nz(r.get("PROP_N")))
    outputRow.put("MASV", nz(isAR ? r.get("PROP_N") : r.get("EPPEXI")))
    
    // Keys / identifiers
    outputRow.put("YEA4", nz(isAR ? r.get("ESYEA4") : r.get("EPYEA4")))
    outputRow.put("JRNO", nz(isAR ? r.get("ESJRNO") : r.get("EPJRNO")))
    outputRow.put("JSNO", nz(isAR ? r.get("ESJSNO") : r.get("EPJSNO")))
    
    // Counterparty & invoice
    outputRow.put("SUNO", nz(isAR ? r.get("ESPYNO") : r.get("EPSUNO")))
    outputRow.put("SINO", nz(isAR ? r.get("ESCINO") : r.get("EPSINO")))
    
    // Names
    outputRow.put("SUNM", nz(isAR ? "" : r.get("IDSUNM")))
    outputRow.put("CUNM", nz(isAR ? r.get("OKCUNM") : ""))
    
    // ---- Opening / Debit / Credit / Running ----
    outputRow.put("OPCR", formatAmount(opening))
    
    double debit = cuam > 0 ? cuam : 0d
    double credit = cuam < 0 ? -cuam : 0d
    
    outputRow.put("DEBT", formatAmount(debit))
    outputRow.put("CRED", formatAmount(credit))
    
    running += (debit - credit)
    outputRow.put("RUNB", formatAmount(running))
    
    // Добавляем в список вместо mi.write()
    allRows.add(outputRow)
}

// Новый метод для сортировки и отправки в output
  void sendToOutput() {
      // Сортировка по ACDT
      allRows.sort { it.ACDT }
      
      // Пересчитываем running balance после сортировки
      running = opening
      Map<String, String> openingBalance = [RUNB: opening.toString(), ACDT: allRows[0].ACDT]
      Map<String, String> closingBalance = [RUNB: allRows[-1].RUNB, ACDT: allRows[-1].ACDT]
    	
    	allRows.add(0, openingBalance)
    	allRows.add(closingBalance)
    	
      allRows.eachWithIndex { row, index ->
          // Пропускаем расчет для первой (0) и последней строки
          if (index != 0 && index != allRows.size() - 1) {
              double debt = toDouble(row.DEBT)
              double cred = toDouble(row.CRED)
              
              running += (debt - cred)
              row.put("RUNB", formatAmount(running))
          } else {
              // Для первой и последней строки просто ставим текущий running
              row.put("RUNB", formatAmount(running))
          }
          
          // Отправляем в mi.outData
          row.each { key, value ->
              mi.outData.put(key, value)
          }
          mi.write()
      }
      
      // Очищаем список
      allRows.clear()
  }

  // ---------------- Helpers ----------------
  private String s(Object o) { return o == null ? "" : o.toString().trim() }
  private String nz(String v) { return v == null ? "" : v.trim() }
  private boolean isEmpty(String x) { return x == null || x.trim().isEmpty() }

  private String firstNZ3(String a, String b, String c) {
    if (!isEmpty(a)) return a
    if (!isEmpty(b)) return b
    if (!isEmpty(c)) return c
    return ""
  }

  private int toInt(Object o) {
    String x = s(o)
    if (isEmpty(x)) return 0
    try { return Integer.parseInt(x) } catch (NumberFormatException nfe) { return 0 }
  }

  /** Primary + default (default may be number or string). */
  private int safeInt2(Object primary, Object defVal) {
    int defInt = 0
    String ds = s(defVal)
    if (!isEmpty(ds)) {
      try { defInt = Integer.parseInt(ds) } catch (NumberFormatException ignore) { defInt = 0 }
    }
    String s1 = s(primary)
    if (isEmpty(s1)) return defInt
    try { return Integer.parseInt(s1) } catch (NumberFormatException ignore) { return defInt }
  }

  private double toDouble(Object o) {
    String x = s(o)
    if (isEmpty(x)) return 0d
    try { return Double.parseDouble(x) } catch (NumberFormatException nfe) { return 0d }
  }

  private String formatAmount(double d) {
    long li = (long) d
    if (Math.abs(d - li) < 0.0000001d) return String.valueOf(li)
    return String.valueOf(d)
  }
}