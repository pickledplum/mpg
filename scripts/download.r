# Download specified FS parameters
library(tools)
library(FactSetOnDemand)
library(xts)
source("/home/honda/mpg/scripts/read_config.r")

download <- function(config_file) {
    ########################################
    # Configure FS
    ########################################
    # Query time out.  Default is 120 secs.  120-3600 secs on client side.
    FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )
    
    ########################################
    # Load config
    ########################################
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
        universe_file <- get("UNIVERSE", mode="character", envir=config) # "list_of_constituents.txt"
        print(paste("Opening:", universe_file))
        conn <- file(universe_file, open="r", blocking=FALSE)
        temp <- read.table(conn, strip.white=TRUE, blank.lines.skip=TRUE, comment.char="#")
        universe <-temp[[1]]
        close(conn)
        
    } else{
        #error
        err_exit(paste("Either", "PORTFOLIO_OFDB", "or", "UNIVERSE", "must be specified"))
    }
    if( exists("PREFIX", envir=config) ){
        prefix <- get("PREFIX", mode="character", envir=config) # "dummy"
    }
    
    stopifnot( exists("T0", envir=config) )
    t0 <- get("T0", mode="character", envir=config) # "1980-01-01" NOTE: implement "last"?
    
    stopifnot( exists("T1", envir=config) )
    t1 <- get("T1", mode="character", envir=config) # "1980-12-12" "now"
    
    stopifnot( exists("DEFAULT_CURRENCY", envir=config) )
    default_currency <- get("DEFAULT_CURRENCY", mode="character", envir=config) # USD
    
    stopifnot( file.exists(output_dir) )
    print(paste("OUTPUT_DIR:", output_dir))
    if( length(universe)== 0 && portfolio_ofdb != "" ){
        # Load the universe from FS portfolio
        data <- FF.ExtractOFDBUniverse(portfolio_ofdb, "0O")
        universe <- data$Id
    }
    #assertWarning( length(universe)>0, verbose=TRUE)
    if( length(universe) == 0){
        print("Warning: Empty universe.  Nothing to do.  Exiting normally...")
        return(0)
    }
    print(paste("UNIVERSE(", length(universe), "): ", paste(universe, collapse=",")))
    if( is.na(prefix) ) {
        print("Warning: No prefix to output files")
    } else{
        print(paste("PREFIX:", prefix))
    }
         
    stopifnot( !is.na(t0))
    stopifnot( !is.na(t1) )
    print(paste("T0,T1:", t0,",",t1))
    stopifnot( !is.na(default_currency) )
    print(paste("DEFAULT_CURRENCY:", default_currency))
    
    ########################################
    # Extract FactSet parameter strings
    # Prefixed with "FACTSET_"
    ########################################
    fs_prefix <- get("FACTSET_PREFIX", envir=config)
    fs_prefix_pattern <- paste("^", fs_prefix, sep="")
    config_param_list <- grep(fs_prefix_pattern, ls(config), value=TRUE)
    

    ########################################
    # Compnay info
    ########################################
    comp.info <- FF.ExtractDataSnapshot(universe, 
                                        "FG_COMPANY_NAME,P_DCOUNTRY,P_REGION,P_EXCHANGE,P_CURRENCY,P_CURRENCY_CODE")
    info_file <- file.path(output_dir, paste(prefix, "company-info.txt",sep="-"))
    write.table(comp.info, info_file, row.names=FALSE, sep=",", quote=TRUE)
    print(paste("Company basic info are written in: ", info_file))
    

    ########################################
    # Download
    ########################################

    started <- proc.time()
    for( config_param in config_param_list ){
        param <- gsub(fs_prefix_pattern, "", config_param)[1]
        controls <- get(config_param, envir=config)
        curr_list <- c(default_currency)
        freq_list <- c()
        if( all("LOCAL" %in% controls) ) {
            if( default_currency != "LOCAL" ){
                curr_list <- cbind(curr_list, "local")
            }
        }
        if( all("USD" %in% controls) ){
            if( default_currency != "USD" ){
                curr_list <- cbind(curr_list, "usd")
            }
        }
        if( all("D" %in% controls) ) {
            freq_list <- cbind(freq_list, "D")
        }
        if( all("M" %in% controls) ) {
            freq_list <- cbind(freq_list, "M")
        }
        if( all("Q" %in% controls) ) {
            freq_list <- cbind(freq_list, "Q")
        }
        if( all("Y" %in% controls) ) {
            freq_list <- cbind(freq_list, "Y")
        }

        for( freq in freq_list ){
            for( isin in universe ) {
                output_filename <- file.path(output_dir, 
                                             paste(
                                                 paste(prefix, param, isin, sep="-"),
                                                 ".csv", sep=""))
                print(output_filename)
                master_data = NULL
                for( curr in curr_list ){
                    tryCatch({
                        data <- FF.ExtractFormulaHistory(isin,param,paste(t0,":",t1, ":",freq), paste("curr=",curr,sep=""))
                    }, error=function(msg){
                        print(paste("ERROR!!!", msg))
                        next
                    })
                    if( nrow(data) < 1 ){
                        print("Empty data.  Skipping...")
                        next
                    }
                    non_nan <- !is.na(data[3])
                    tseries <- as.data.frame(cbind(data[2][non_nan], data[3][non_nan]))
                    colnames(tseries) <- c(paste("nr",nrow(tseries),sep=""), curr)
                    
                    if( !is.null(master_data) ){
                        
                        tseries <- merge(x=master_data, 
                                         y=tseries, 
                                         by.x=colnames(tseries)[1],
                                         by.y=colnames(master_data)[1],
                                         all=TRUE)
                    }
                    else{
                        master_data <- tseries
                    }
                }
                
                tryCatch({
                    fout <- file(output_filename, "w", blocking=FALSE)
                    write.table(master_data, fout,
                                quote=FALSE, 
                                row.names=FALSE, 
                                col.names=TRUE,
                                sep=",")
                    
                    
                }, error=function(msg){
                    print(paste("ERROR!!!", msg))
                    return(NULL)
                }, finally=function(fout){
                    close(fout)
                })
                close(fout)
            }

        }

    }
    ended <- proc.time()
    
    
    return(0)
}
config_file <- "/home/honda/mpg/dummy/params.conf"
download(config_file)