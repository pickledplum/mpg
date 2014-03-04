tryCatch({
    library(tools)
    library(FactSetOnDemand)
    library(xts)
    library(RSQLite)
    
    source("read_config.r")
    source("logger.r")
    source("tryDb.r")
    source("assert.r")
    source("tryExtract.r")
    source("is.empty.r")
    source("drop_tables.r")
    source("julianday.r")
    source("create_year_summary.r")
    source("enQuote.r")
    
}, warning=function(msg){
    print(msg)
    stop()
}, error=function(msg){
    print(msg)
    stop()
}
) 
#####################################
# Constants
#####################################
portfolio_ofdb = "PERSONAL:HONDA/JAPAN500"

tag <- "copy-japan500"
dbdir <- file.path("/home/honda/sqlite-db", tag)
if( !file.exists(dbdir) ){
    dir.create(dbdir)
}
wkdir <- dbdir
if( !file.exists(wkdir) ){
    dir.create(dbdir)
}
dbname <- paste(tag, ".sqlite", sep="")
dbpath <- file.path(dbdir, dbname)

logfile_name <- paste(tag, ".log", sep="")
do_stdout <- TRUE


#####################################
# Set up working directory
#####################################
if( !file.exists(wkdir) ){
    print(paste("Directory does not exist.  Creating...:", wkdir))
    dir.create(wkdir)
}
stopifnot( file.exists(wkdir) )

#####################################
# Open Logger
#####################################
logfile <- file.path(wkdir, logfile_name)
logger.init(level=logger.DEBUG,
            do_stdout=do_stdout,
            logfile=logfile)

print(paste("Log file:", logfile))

#####################################
# Open DB
#####################################
conn <- dbConnect( SQLite(), dbpath )
logger.warn(paste("Opened SQLite database:", dbpath))

########################################
# Configure FS
########################################
# Query time out.  Default is 120 secs.  120-3600 secs on client side.
FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )
logger.info(paste("FactSet timetout:", 900, "secs"))

########################################
#  FQLs 
########################################

fql.syntax <- c("FG_COMPANY_NAME",
                      "P_DCOUNTRY",
                      "P_DCOUNTRY(ISO3)",    
                      "P_DCOUNTRY(REG)",
                      "FF_CURN_LOC",
                      "FF_CURN_ISO",
                      "P_EXCHANGE(NAME,FACTSET)",
                      "P_EXcHANGE(CODE,FACTSET)",
                      "P_EXCOUNTRY",
                      "P_EXCOUNTRY(ISO3)",
                      "P_EXCOUNTRY(REG)",
                      "P_CURRENCY(NAME)",
                      "P_CURRENCY(ISO)",
                      "FG_FACTSET_SECTOR",
                      "FF_MAJOR_IND_NAME",
                      "FF_MAJOR_SUBIND_NAME",
                      "FE_COMPANY_INFO(ISIN)",
                      "FE_COMPANY_INFO(SEDOL)")
fql.name <- c(
    "id", 
    "name", 
    "domicile_country",
    "domicile_country_iso3", 
    "domicile_region",
    "domicile_curr",
    "domicile_curr_id", 
    "exchange",
    "exchange_id",
    "exchange_country",
    "exchange_country_iso3",
    "exchange_region",
    "exchange_curr", 
    "exchange_curr_id", 
    "sector",
    "industry",
    "subind",
    "isin", 
    "sedol")
########################################
# Currency table
########################################
curr.column.name <- c(
    "curr_id",
    "currency"
)
curr.column.type <- c("CHAR(3) PRIMARY KEY NOT NULL UNIQUE",
                      "TEXT(50)"
)
curr.column.constraint <- c()                               

curr.specs <- c(paste(curr.column.name, curr.column.type), curr.column.constraint)
tryCreateTableIfNotExists(conn, "currency", curr.specs)
logger.info(paste("Created CURRENCY table:", paste(curr.specs, collapse=",")))

########################################
# Country table
########################################
country.column.name <- c(
    "country_id",
    "country_name",
    "region",
    "curr_id",
    "market_id"
)
country.column.type <- c("CHAR(2) PRIMARY KEY NOT NULL UNIQUE", 
                              "TEXT(100)", 
                              "TEXT(100)",
                              "CHAR(3)",
                              "VARCHAR(5)"
)
country.column.constraint <- c("FOREIGN KEY (market_id) REFERENCES market (market_id) ON DELETE NO ACTION ON UPDATE CASCADE")                               

country.specs <- c(paste(country.column.name, country.column.type), country.column.constraint)
tryCreateTableIfNotExists(conn, "temp_country", country.specs)
logger.info(paste("Created TEMP_COUNTRY table:", paste(country.specs, collapse=",")))

########################################
# Market table
########################################
market.column.name <- c(
        "market_id",
        "market"
)
market.column.type <- c(
        "VARCHAR(5) PRIMARY KEY NOT NULL UNIQUE",
        "TEXT(50)"
)
market.column.constraint <- c()

market.specs <- c(paste(market.column.name, market.column.type), market.column.constraint)
tryCreateTableIfNotExists(conn, "market", market.specs)
logger.info(paste("Created MARKET table:", paste(market.specs, collapse=",")))

########################################
# Exchange table
########################################
exchange.column.name <- c(
    "exchange_id",
    "exchange_name",
    "country_id",
    "curr_id"
)
exchange.column.type <- c("VARCHAR(5) PRIMARY KEY NOT NULL UNIQUE", 
                         "TEXT(100)",
                         "CHAR(2)",
                         "CHAR(3)"
)
exchange.column.constraint <- c()                                                           

exchange.specs <- c(paste(exchange.column.name, exchange.column.type), exchange.column.constraint)
tryCreateTableIfNotExists(conn, "exchange", exchange.specs)
logger.info(paste("Created EXCHANGE table:", paste(exchange.specs, collapse=",")))

########################################
# Company table
########################################
company.column.name <- c(
    "factset_id",
    "company",
    "sector",
    "industry",
    "subind",
    "domicile_country_id",
    "exchange_id",
    "sedol",
    "isin"
)

company.column.type <- c("VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
                              "TEXT(100)", 
                              "TEXT(100)",
                              "TEXT(100)",
                              "TEXT(100)",
                              "CHAR(2)",
                              "VARCHAR(5)",
                              "VARCHAR(20)",
                              "VARCHAR(20)"
)
company.column.constraint <- c("FOREIGN KEY (domicile_country_id) REFERENCES country (country_id) ON DELETE NO ACTION ON UPDATE CASCADE",
                                "FOREIGN KEY (exchange_id) REFERENCES exchange (exchange_id) ON DELETE NO ACTION ON UPDATE CASCADE"                         
)
company.specs <- c(paste(company.column.name, company.column.type), company.column.constraint)
tryCreateTableIfNotExists(conn, "temp_company", company.specs)
logger.info(paste("Created TEMP_COMPANY table:", paste(company.specs, collapse=",")))


BINSIZE = 100
if( FALSE ){
    
    data <- FF.ExtractOFDBUniverse(portfolio_ofdb, "0O")
    universe <- data$Id
    logger.info(paste("Loaded universe from", portfolio_ofdb))
    
    i <- 1
    for( begin in seq(1, length(universe), BINSIZE )){
        end <- min(begin+BINSIZE-1, length(universe))
        
        mini_universe <- universe[begin:end]
        # Get companies meta data
        all_company <- FF.ExtractDataSnapshot(mini_universe, paste(fql.syntax, collapse=","))
        if( is.empty(all_company) ){
            stop(paste("Empty company meta data:", mini_universe))
        }
        all_company$Date <- c()
        if( ncol(all_company) != length(fql.name) ) {
            stop(paste("Collapted company meta data within:", mini_universe))
        }
        colnames(all_company) <- fql.name 
        
        dbWriteTable(conn, "meta", all_company, append=TRUE)
        i <- i+1
    }
}

if( FALSE ){
    names <- c("market_id", "market")
    vals <- data.frame(c("DM", "EM", "FM"), c("developed", "emerging", "frontier"))
    tryBulkInsertOrReplace(conn, names, vals)
    
}
if( FALSE ){
    data <- tryGetQuery(conn, "SELECT id FROM meta")
    universe <- data$id
    
    data <- tryGetQuery(conn, "SELECT DISTINCT T1.curr_code AS curr_code, T1.curr AS curr FROM (SELECT DISTINCT domicile_curr_code AS curr_code, domicile_curr AS curr FROM meta) AS T1 JOIN (SELECT DISTINCT exchange_curr_code AS curr_code, exchange_curr AS curr FROM meta) AS T2 USING(curr_code)")
    for( i in nrow(data) ){
        entry <- data[i,]
        curr_id <- entry$curr_code
        curr <- entry$curr
        names <- c("curr_id", "currency")
        vals <- enQuote(c(curr_id, curr))
        tryInsertOrReplace(conn, "currency", names, vals)
    }
    data <- tryGetQuery(conn, "SELECT DISTINCT exchange, exchange_code, exchange_country, exchange_country_iso3, exchange_region, exchange_curr, exchange_curr_code FROM meta GROUP BY exchange_code")
    
    for( i in seq(1,nrow(data)) ){
        entry <- data[i,]
        exchange <- entry$exchange
        exchange_id <- entry$exchange_code
        country_id <- entry$exchange_country_iso3
        curr_id <- entry$exchange_curr_code  
        names <- c("exchange_id", "exchange_name", "country_id", "curr_id")
        vals <- enQuote(c(exchange, exchange_id, country_id, curr_id))              
        tryInsertOrReplace(conn, "exchange", names, vals)
    }
    
    data <- tryGetQuery(conn, "SELECT DISTINCT T1.country AS country, T1.country_code AS country_code, T1.region AS region, T1.curr AS curr, T1.curr_code AS curr_code FROM (SELECT DISTINCT domicile_country AS country, domicile_country_iso3 AS country_code, domicile_region AS region, domicile_curr AS curr, domicile_curr_code AS curr_code FROM meta) AS T1 JOIN (SELECT DISTINCT exchange_country AS country, exchange_country_iso3 AS country_code, exchange_region AS region, exchange_curr AS curr, exchange_curr_code AS curr_code FROM meta) AS T2 USING(country_code)")
    for( i in seq(1,nrow(data)) ){
        entry <- data[i,]
        country <- entry$country
        country_id <- entry$country_code
        region <- entry$region
        curr_id <- entry$curr_code
        names <- c("country_id", "country_name", "region", "curr_id")
        vals <- enQuote(c(country, country_id, region, curr_id))
        tryInsertOrReplace(conn, "temp_country", names, vals)
    }
    
    data <- tryGetQuery(conn, "SELECT id, name, domicile_country_iso3, exchange_code, sector, industry, subind, isin, sedol FROM meta")
    for( i in seq(1,nrow(data)) ){
        entry <- data[i,]
        factset_id <- entry$id
        company <- entry$name
    
        sector <- entry$sector
        industry <- entry$industry
        subind <- entry$subind
        isin <- entry$isin
        sedol <- entry$sedol
        domicile_country_id <- entry$domicile_country_iso3
        exchange_id <- entry$exchange_code
        
    
        names <- c("factset_id", "company", "sector", "industry", "subind", "sedol", "isin", "domicile_country_id", "exchange_id")
        vals <- enQuote(c(factset_id,company,sector,industry,subind,sedol,isin,domicile_country_id,exchange_id))
        tryInsertOrReplace(conn, "temp_company", names, vals)
        
    }
}


tryCreateTableIfNotExists(conn, "country", country.specs)
logger.info(paste("Created COUNTRY table:", paste(country.specs, collapse=",")))

data <- trySelect(conn, "temp_country", c("*"), c())
for( i in seq(1, nrow(data) )){
    names <- c("country_id", "country_name", "region", "curr_id", "market_id")
    entry <- data[i,]
    vals <- enQuote(c(
        entry$country_id,
        entry$country_name,
        entry$region,
        entry$curr_id,
        entry$market_id
    ))
    tryInsertOrReplace(conn, "country", names, vals)
}

tryCreateTableIfNotExists(conn, "company", company.specs)
logger.info(paste("Created COMPANY table:", paste(company.specs, collapse=",")))

data <- trySelect(conn, "temp_company", c("*"), c())
for( i in seq(1, nrow(data) ) ){
    names <- c("factset_id", "company", "sector", "industry", "subind", "domicile_country_id", "exchange_id", "sedol", "isin")
    entry <- data[i,]
    vals <- enQuote(c(
        entry$factset_id,
        entry$company,
        entry$sector,
        entry$industry,
        entry$subind,
        entry$domicile_country_id,
        entry$exchange_id,
        entry$sedol,
        entry$isin
    ))
    tryInsertOrReplace(conn, "company", names, vals)
}

dbDisconnect(conn)
