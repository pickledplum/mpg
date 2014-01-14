source("read_config.r")
source("download.r")
source("populate_data.r")
source("logger.r")
source("drop_tables.r")
source("populate_company.r")

exit_f <- function(){
    dbDisconnect(conn)
}


db <- "D:/home/honda/sqlite-db/financial.sqlite3"
config_file <- "/home/honda/mpg/dummy/params.conf"
config <- read_config(config_file) # returns an environment
output_root <- get("OUTPUT_ROOT", envir=config)
#on.exit(exit_f)

timestamp <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
logger <<- file.path(output_root, paste("dummy", ".log",sep=""))
print(paste("Log file:", logger))

conn <<- dbConnect( SQLite(), db )
log.info(paste("Opened SQLite database:", db))

download(config_file)
log.info("Finished downloading data from FactSet servers...")

# Populate company data table
company_file_list <- c( "D:/home/honda/mpg/dummy/fs_output/dummy-company-info.txt",
                        "D:/home/honda/mpg/acwi/fs_output/etf-acwi-company-info.txt",
                        "D:/home/honda/mpg/frontier/fs_output/ex-fm-company-info.txt",
                        "D:/home/honda/mpg/emerging/fs_output/ex-em-company-info.txt",
                        "D:/home/honda/mpg/developed/fs_output/ex-dm-company-info.txt" 
)
market_designations <- c("dummy", "NULL", "fm", "em", "dm")
drop_tables(conn, exclude=c())
log.info("Finished dropping tables")


populate_company(conn, company_file_list, market_designations)
log.info("Finished populating country, company tables...")

populate_data(config_file, conn)
log.info("Finished populating time series tables...")

dbDisconnect(conn)
