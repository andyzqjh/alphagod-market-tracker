# Stock universe for pre-market gapper scanning (~400 liquid stocks)
STOCK_UNIVERSE = [
    # Mega cap tech
    "AAPL","MSFT","NVDA","GOOG","GOOGL","META","AMZN","TSLA","AVGO","ORCL",
    # Large cap tech
    "AMD","INTC","QCOM","TXN","MU","WDC","STX","MRVL","ARM","SMCI",
    "DELL","HPE","NTAP","PURE","CSCO","ANET","JNPR","MSI","FFIV",
    # Semiconductors
    "LRCX","AMAT","KLAC","TER","FORM","ONTO","ACLS","CAMT","ICHR","ENTG","SNPS","CDNS",
    # AI / Data / Cloud
    "NET","SNOW","DDOG","MDB","ESTC","ZS","CRWD","PANW","S","FTNT","OKTA","CYBR",
    "CRM","NOW","ADBE","INTU","WDAY","TEAM","HUBS","SHOP","TWLO","VEEV",
    "APP","FSLY","TTMI","BILL","DOCN","CFLT","GTLB","SMAR",
    # Fiber / Optical
    "AXTI","AAOI","LITE","CIEN","COHR","VIAV","INFN","IIVI","FNSR",
    # Memory / Storage
    "SNDK","NAND",
    # AI Energy
    "BE","AMPX","POWL","FTAI","TE","FCEL","PLUG","BLDP","RUN","ENPH","SEDG",
    # Cooling / Infra
    "VRT","CLS","AAON","GRMN",
    # Crypto related
    "COIN","HOOD","MSTR","RIOT","MARA","CLSK","CIFR","HUT","IREN","APLD","CORZ","BTBT",
    # Data Centers / REITs
    "EQIX","DLR","AMT","NBIS",
    # Defense / Aerospace
    "LMT","RTX","NOC","GD","KTOS","PLTR","CACI","SAIC","LDOS","BAH","RKLB","ASTS","PL",
    "SPCE","MNTS","IRDM","MAXR",
    # Biotech
    "MRNA","BNTX","VRTX","REGN","BIIB","AMGN","GILD","BMY","ABBV","LLY",
    "HIMS","QURE","BHVN","CAPR","UMAC","SEER","BEAM","EDIT","CRSP","NTLA",
    "ALNY","RARE","ACAD","SAGE","PRGO","JAZZ","INVA","IMVT","KYMR","PTGX",
    "RCUS","SNDX","AGEN","ARDX","ACMR","TGTX","PRAX","NUVL","ARVN","KRTX",
    # EV / Clean Energy
    "RIVN","LCID","NIO","XPEV","LI","CHPT","BLNK","EVGO","FSR","GOEV","NKLA","PTRA",
    "WKHS","SOLO","AYRO","IDEX","HYZN","HYLN","FFIE","MULN",
    # Financials
    "JPM","BAC","GS","MS","WFC","C","V","MA","AXP","BLK","SCHW","IBKR",
    "SOFI","AFRM","UPST","LC","OPEN","OPFI","CURO","PRAA",
    # Consumer / Retail
    "WMT","TGT","COST","HD","LOW","AMZN","BABA","JD","PDD","MELI","SE",
    "MCD","SBUX","YUM","CMG","DPZ","WING","SHAK",
    "NKE","LULU","UAA","PVH","RL","VFC","ONON","BIRK",
    # Media / Entertainment
    "NFLX","DIS","PARA","WBD","SPOT","RBLX","U","EA","TTWO","ATVI",
    # Healthcare
    "JNJ","UNH","PFE","MRK","ABT","MDT","ISRG","DXCM","PODD","ALGN",
    "TMO","DHR","IQV","CRL","A","IDXX","MLAB","NTRA","ILMN","PACB",
    # Oil & Gas
    "XOM","CVX","OXY","COP","EOG","SLB","HAL","BKR","MRO","DVN","FANG",
    "WTI","BATL","ALTO","CLNE","GEVO","REX","CDEV","VTLE","ESTE",
    # Small cap momentum / popular
    "HIMS","GME","AMC","BBBY","KOSS","EXPR","NAKD","CLOV","WISH","WKHS",
    "PDYN","TTD","NAGE","RTO","CBRL","KC","ALTO","SOLS","SNDK",
    # Misc popular
    "ABNB","UBER","LYFT","DASH","GRAB","GRAB","RDFN","OPEN","OFFERPAD",
    "SPCE","ACHR","JOBY","LILM","WATT","LAZR","INVZ","OUST","VLDR","LIDR",
    "ZM","DOCU","PTON","NKLA","RIDE","XPEV","BLNK","FVRR","UPWK",
    "DKNG","PENN","MGAM","GENI","SGMS","IGT","EVERI","RSI",
    "AFRM","LMND","ROOT","HI","KINS","METC","BARK","COOK","ATAI","SAVA",
    "SDCL","JMIA","TIGR","FUTU","NOAH","LQDT","PRPL","LESL","FIGS","BROS",
]

# Remove duplicates
STOCK_UNIVERSE = list(dict.fromkeys(STOCK_UNIVERSE))
