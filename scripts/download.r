# Download specified FS parameters
library(tools)

source("/home/honda/mpg/scripts/read_config.r")

download <- function(config_file) {
    config <- read_config(config_file) # returns an environment
    
    output_dir <- ""
    portfolio_ofdb <- ""
    universe <- c()
    prefix <- ""
    t0 <- ""
    t1 <- ""
    default_currency <- ""
    
    stopifnot( exists("OUTPUT_DIR", envir=config) )
    output_dir <- get("OUTPUT_DIR", mode="character", envir=config) # "D:/home/honda/mpg/dummy/fs_output"
    
    if( exists("PORTFOLIO_OFDB", envir=config) ){
        portfolio_ofdb <- get("PORTFOLIO_OFDB", mode="character", envir=config) # "PERSONAL:HONDA_MSCI_ACWI_ETF"
    } else if( exists("UNIVERSE", envir=config) ){
        universe <- get("PORTFOLIO_LIST", mode="character", envir=config) # "list_of_constituents.txt"
    } else{
        #error
        err_exit(paste("Either", "PORTFOLIO_OFDB", "or", "UNIVERSE", "must be specified"))
    }
    if( exists("PREFIX", envir=config) ){
        prefix <- get("PREFIX", mode="character", envir=config) # "dummy"
    }
    
    stopifnot( exists("T0", envir=config) )
    t0 <- get("T0", mode="numeric", envir=config) # "1980-01-01" NOTE: implement "last"?
    
    stopifnot( exists("T1", envir=config) )
    t1 <- get("T1", mode="numeric", envir=config) # "1980-12-12" "now"
    
    stopifnot( exists("DEFAULT_CURRENCY", envir=config) )
    default_currency <- get("DEFAULT_CURRENCY", mode="character", envir=config) # USD
    
    stopifnot( file.exists(output_dir) )
    print(paste("OUTPUT_DIR:", output_dir))
    if( !is.na(portfolio_ofdb ) ){
        # download the list of constituents
        universe <- c()
    }
    #assertWarning( length(universe)>0, verbose=TRUE)
    if( is.null(universe) ){
        print("Warning: Empty universe.  Nothing to do.  Exiting normally...")
        return(0)
    }
    if( !is.na(prefix) ) {
        print("Warning: No prefix to output files")
    }
    stopifnot( !is.na(t0))
    stopifnot( !is.na(t1) )
    stopifnot( !is.na(default_currency) )

    return(0)
}

config_file <- "/home/honda/mpg/dummy/params.conf"
download(config_file)