#'
#' Define tables in META db
#'
#'
source("tryDb.r")
source("logger.r")
source("dropTables.r")

#'
#' Create CATALOG table
#'
create.table.catalog <- function(conn) {
    #' Define CATALOG table
    catalog.attrs <- c( "dbname     VARCHAR(30)",
                        "tablename  VARCHAR(41)",
                        "factset_id VARCHAR(20) NOT NULL",
                        "fql        VARCHAR(20) NOT NULL",
                        "usd        BOOL",
                        "local      BOOL",
                        "earliest   INTEGER",
                        "latest     INTEGER")
    
    catalog.constraints <- c("PRIMARY KEY(factset_id, fql)",
                             "FOREIGN KEY(factset_id) REFERENCES company(factset_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
                             "FOREIGN KEY(fql) REFERENCES fql(fql) ON DELETE NO ACTION ON UPDATE CASCADE")
    
    catalog.specs <- c(catalog.attrs, catalog.constraints)
    tryCreateTable(conn, "catalog", catalog.specs)
    logger.info(paste("Created CATALOG table:", paste(catalog.specs, collapse=",")))
}
#'
#'Create & fill CATEGORY table
#'
create.table.category <- function(conn){

    category.attrs <- c("category_id          VARCHAR(50) PRIMARY KEY NOT NULL UNIQUE", 
                        "category_description TEXT(50)")
    category.specs <- paste(category.attrs)
    tryCreateTableIfNotExists(conn, "category", category.specs)
    logger.info("Created CATEGORY table")

    category.category_id <- c("company_fund", 
                              "price", 
                              "company_info", 
                              "country_fund")
    category.category_descript <- c("company fundamental",
                                    "price",
                                    "company info",
                                    "country fundamental")
    tryBulkInsertOrReplace(conn, 
                           "category", 
                           c("category_id", "category_description"), 
                           data.frame(enQuote(category.category_id), 
                                      enQuote(category.category_descript)))
    
    logger.info(paste("Created & filled CATEGORY table:", paste(category.specs, collapse=",")))
}
#'
#' Create COMPANY table
#' 
create.table.company <- function(conn) {
    company.attrs <- c("factset_id  VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE",
                       "company     TEXT(100)",
                       "sector      TEXT(100)",
                       "industry    TEXT(100)",
                       "subind      TEXT(100)",
                       "country_id  CHAR(3)",
                       "exchange_id VARCHAR(5)",
                       "sedol       VARCHAR(20)",
                       "isin        VARCHAR(20)"
    )
    company.constraints <- c("FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
                             "FOREIGN KEY(exchange_id) REFERENCES exchange(exchange_id) ON DELETE NO ACTION ON UPDATE CASCADE"
    )
    company.specs <- c(company.attrs, company.constraints)
    tryCreateTable(conn, "company", company.specs)
    logger.info(paste("Created COMPANY table:", paste(company.specs, collapse=",")))
}

#'
#' Create COUNTRY table
#' 
create.table.country <- function(conn) {
    country.attrs <- c("country_id  CHAR(3)  PRIMARY KEY NOT NULL UNIQUE",
                       "country     TEXT(100)",
                       "region      TEXT(100)",
                       "curr_id     CHAR(3)",
                       "market_id   VARCHAR(5)"
    )
    country.constraints <- c("FOREIGN KEY(curr_id) REFERENCES currency(curr_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
                             "FOREIGN KEY(market_id) REFERENCES market(market_id) ON DELETE NO ACTION ON UPDATE CASCADE"
    )
    country.specs <- c(country.attrs, country.constraints)
    tryCreateTable(conn, "country", country.specs)
    logger.info(paste("Created COUNTRY table:", paste(country.specs, collapse=",")))
}

create.table.currency <- function(conn){
    currency.attrs <- c("curr_id CHAR(3) PRIMARY KEY NOT NULL UNIQUE",
                        "currency TEXT(50)"
    )
    currency.specs <- currency.attrs
    tryCreateTable(conn, "currency", currency.specs)
    logger.info(paste("Created CURRENCY table:", paste(currency.specs, collapse=",")))
}

create.table.exchange <- function(conn){
    exchange.attrs <- c("exchange_id VARCHAR(5) PRIMARY KEY NOT NULL UNIQUE",
                        "exchange               TEXT(100)",
                        "country_id             CHAR(3)",
                        "curr_id                CHAR(3)"
    )
    exchange.constraints <- c("FOREIGN KEY(country_id) REFERENCES country(country_id) ON DELETE NO ACTION ON UPDATE CASCADE",
                              "FOREIGN KEY(curr_id) REFERENCES currency(curr_id) ON DELETE NO ACTION ON UPDATE CASCADE"
    )
    exchange.specs <- c(exchange.attrs, exchange.constraints)
    tryCreateTable(conn, "exchange", exchange.specs)
    logger.info(paste("Created EXCHANGE table:", paste(exchange.specs, collapse=",")))
}
create.table.frequency <- function(conn) {
    frequency.attrs <- c("freq VARCHAR(1)  PRIMARY KEY NOT NULL UNIQUE", 
                         "freq_description VARCHAR(20)"
    )
    
    frequency.specs <- frequency.attrs
    
    tryCreateTableIfNotExists(conn, "frequency", frequency.specs)
    logger.info("Created FREQUENCY table")
    
    tryBulkInsertOrReplace(conn, "frequency", 
                           c("freq", "freq_description"), 
                           data.frame(enQuote(c("Y","S","Q","M","D")),
                                      enQuote(c("Anuual","Semiannual","Quarterly","Monthly","Daily"))) )
    logger.info(paste("Created & filled FREQUENCY table:", paste(frequency.specs, collapse=",")))
}
create.table.market <- function(conn) {
    market.attrs <- c("market_id VARCHAR(5) PRIMARY KEY NOT NULL UNIQUE", 
                      "market               TEXT(50)"
    )
    
    market.specs <- market.attrs
    
    tryCreateTableIfNotExists(conn, "market", market.specs)
    logger.info("Created market table")
    
    tryBulkInsertOrReplace(conn, "market", 
                           c("market_id", "market"), 
                           data.frame(enQuote(c("DM","EM","FM","WORLD")),
                                      enQuote(c("developed","emerging","frontier","world"))) )
    logger.info(paste("Created & filled MARKET table:", paste(market.specs, collapse=",")))
}

create.table.fql <- function(conn, fql_map_csv){

    ########################################
    # Create FQL table
    ########################################
    fql.column.name <- c("fql", 
                         "syntax", 
                         "description", 
                         "unit", 
                         "freq",
                         "category_id", 
                         "note")
    fql.column.type <- c("VARCHAR(20) PRIMARY KEY NOT NULL UNIQUE", 
                         "TEXT(200)",
                         "TEXT(100)", 
                         "FLOAT", 
                         "CHAR(1)", 
                         "VARCHAR(50)",
                         "VARCHAR(200)")
    fql.column.constraint <-  c("FOREIGN KEY(category_id) REFERENCES category(category_id) ON DELETE NO ACTION ON UPDATE CASCADE", 
                                "FOREIGN KEY(freq) REFERENCES frequency(freq) ON DELETE NO ACTION ON UPDATE CASCADE")
    
    fql.specs <- c(paste(fql.column.name, fql.column.type), fql.column.constraint)
    tryCreateTableIfNotExists(conn, "fql", fql.specs)
    logger.info(paste("Created FQL table:", paste(fql.specs, collapse=",")))
    
    stopifnot(file.exists(fql_map_csv))
    fql_map <- read.csv(fql_map_csv)
    rownames(fql_map) <- fql_map$fql
    tryBulkInsertOrReplace(conn, "fql", 
                           fql.column.name,
                           data.frame(enQuote2(fql_map$fql),
                                      enQuote2(fql_map$syntax),
                                      enQuote(fql_map$description),
                                      fql_map$unit,
                                      enQuote(fql_map$freq),
                                      enQuote(fql_map$category_id),
                                      enQuote(fql_map$note)
                           ))
    
    logger.info(paste("Populated FQL table:", nrow(fql_map)))
}
fill.table.catalog <- function(target.conn, source.conn) {
   # simply tranfer from source to target
}

fill.table.company <- function(target.conn, source.conn) {
    # pull from FS server
    #  sector
    #  industry
    #  subind
    #  domicile country
    #  exchange
    # transfer
    #  sedol
    #  isin
    
}
fill.table.country <- function(target.conn, country_code_table) {

    country_code_table[[1]] <- enQuote(country_code_table[[1]])
    country_code_table[[2]] <- enQuote(country_code_table[[2]])
    country_code_table[[3]] <- enQuote(country_code_table[[3]])
    tryBulkInsertOrReplace(target.conn, "country", c("country_id", "country", "market_id"), country_code_table[c(1,2,3)], 1)

}
fill.table.currency <- function(target.conn, currency_code_table) {
    currency_code_table[[1]] <- enQuote(currency_code_table[[1]])
    currency_code_table[[2]] <- enQuote(currency_code_table[[2]])
    tryBulkInsertOrReplace(target.conn, "currency", c("curr_id", "currency"), currency_code_table[c(1,2)], 1)
    
}
fill.table.exchange <- function(target.conn, source.conn ){
}


target.dbdir <- "/home/honda/sqlite-db/new"
target.dbname <- "meta.sqlite"
source.dbdir <- "/home/honda/sqlite-db/developed"
source.dbname <- "developed.sqlite"
fql.map.file <- "/home/honda/mpg/fql_map.csv"

if( !file.exists(target.dbdir) ){
    dir.create(target.dbdir, recursive=TRUE)
}
#####################################
# Open Logger
#####################################
logger.init(level=logger.DEBUG,
            do_stdout=TRUE)


target.conn <- dbConnect(SQLite(), file.path(target.dbdir, target.dbname))
source.conn <- dbConnect(SQLite(), file.path(source.dbdir, source.dbname))

dropTables(target.conn)

create.table.catalog(target.conn)
create.table.category(target.conn) # filled
create.table.company(target.conn)
create.table.country(target.conn)
create.table.frequency(target.conn)  # filled
create.table.fql(target.conn, fql_map_csv=fql.map.file) #filled
create.table.currency(target.conn)
create.table.market(target.conn) # filled
create.table.exchange(target.conn)


fill.table.catalog(target.conn, source.conn)
fill.table.company(target.conn, source.conn)
curr_table <- read.csv("/home/honda/mpg/curr_code.csv")
fill.table.currency(target.conn, curr_table)
country_table <- read.csv("/home/honda/mpg/country_code.csv")
fill.table.country(target.conn, country_table)
#fill.table.exchange(target.conn, source.conn)

dbDisconnect(target.conn)
dbDisconnect(source.conn)


