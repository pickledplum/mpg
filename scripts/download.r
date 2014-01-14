# Download specified FS parameters
library(tools)
library(FactSetOnDemand)
library(xts)
#source("read_config.r")
#source('logger.r"')

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
    
    output_root <- ""
    portfolio_ofdb <- ""
    universe <- c()
    prefix <- ""
    t0 <- ""
    t1 <- ""
    default_currency <- ""
    
    stopifnot( exists("OUTPUT_ROOT", envir=config) )
    output_root <- get("OUTPUT_ROOT", mode="character", envir=config) # "D:/home/honda/mpg/dummy/fs_output"
    
    if( exists("PORTFOLIO_OFDB", envir=config) ){
        portfolio_ofdb <- get("PORTFOLIO_OFDB", mode="character", envir=config) # "PERSONAL:HONDA_MSCI_ACWI_ETF"
    } else if( exists("UNIVERSE", envir=config) ){
        universe_file <- get("UNIVERSE", mode="character", envir=config) # "list_of_constituents.txt"
        print(paste("Opening:", universe_file))
        stopifnot( file.exists(universe_file))
        unicon <- file(universe_file, open="r", blocking=FALSE)
        temp <- read.table(unicon, colClasses=c("character"), header=TRUE, strip.white=TRUE, blank.lines.skip=TRUE, comment.char="#")
        universe <-temp[[1]]
        close(unicon)
        
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
    
    stopifnot( file.exists(output_root) )
    print(paste("output_root:", output_root))
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
    config_param_list <- config_param_list[-c(which(config_param_list=="FACTSET_PREFIX"))]   
    param_list <- gsub(fs_prefix_pattern, "", config_param_list)
    
    ########################################
    # Compnay info
    ########################################
    comp.info <- FF.ExtractDataSnapshot(universe, 
                                        "FG_COMPANY_NAME,P_DCOUNTRY,P_REGION,P_EXCHANGE,P_CURRENCY,P_CURRENCY_CODE")
    info_file <- file.path(output_root, paste(prefix, "company-info.txt",sep="-"))
    write.table(comp.info, info_file, row.names=FALSE, sep=",", quote=TRUE)
    print(paste("Company basic info are written in: ", info_file))
    

    ########################################
    # Download
    ########################################

    started <- proc.time()
    for( param in param_list ){
        # create a subdirectory for this param
        output_dir <- file.path(output_root, param)
        if( !file.exists(output_dir) ){
            dir.create(output_dir, showWarnings=TRUE, recursive=FALSE, mode="0775")
        }
        
        controls <- get(paste(fs_prefix, param, sep=""), envir=config)
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
                                                 paste(param, as.character(isin), sep="-"),
                                                 ".csv", sep=""))
                log.info(output_filename)
                master_data = NULL
                #browser()
                for( curr in curr_list ){
                    tryCatch({
                        if( grepl("^P_", param) ){
                            fs_str <- paste(param, "(", paste(t0,t1,freq,curr,sep=","),")",sep="")
                            str <- paste(paste("FF.ExtractFormulaHistory(\"",as.character(isin),"\"",sep=""), ",", fs_str,")",sep="")
                            # FF.ExtractFormulaHistory("002826", P_PRICE_AVG(20120101,20121231,D,USD))
                            data <- FF.ExtractFormulaHistory(as.character(isin), fs_str)
                            
                        } else if( grepl("^FF_", param) || grepl("^FG_", param)){
                            # FF.ExtractFormulaHistory("002826", "P_PRICE_AVG", "20120101:20121231:D","curr=USD"))
                            data <- FF.ExtractFormulaHistory(as.character(isin),
                                                         param,
                                                         paste(t0,t1,freq,sep=":"),
                                                         paste("curr=",curr,sep=""))
                        } else {
                            print(paste("I don't know what to do with this FS param:", param))
                        }

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
                        master_data <- merge(x=master_data, 
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
                    colnames(master_data) <- c(paste("nr",nrow(master_data),sep=""),
                                               colnames(master_data)[2:ncol(master_data)])
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
    print(paste("Ellapsed time:", (ended-started)[3]))
    return(0)
}