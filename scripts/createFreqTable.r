source("tryDb.r")
source("logger.r")
createFreqTable <- function(conn){

    ########################################
    # Create FREQUENCY table
    ########################################
    frequency.column.name <- c("freq", 
                               "freq_name")
    
    frequency.column.type <- c("VARCHAR(1) PRIMARY KEY NOT NULL UNIQUE",
                               "VARCHAR(20) UNIQUE"
    )
    frequency.specs <- paste(frequency.column.name, frequency.column.type)
    
    tryCreateTableIfNotExists(conn, "frequency", frequency.specs)
    logger.info("Created FREQUENCY table")
    
    tryBulkInsertOrReplace(conn, "frequency", 
                           frequency.column.name, 
                           data.frame(enQuote(c("Y","S","Q","M","D")),
                                      enQuote(c("Anuual","Semiannual","Quarterly","Monthly","Daily"))) )
    logger.info(paste("Created FREQUENCY table:", paste(frequency.specs, collapse=",")))
}
