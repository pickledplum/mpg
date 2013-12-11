# search through the directory for each FS parameter
# collect the list of IDs for each
# find out the last data point
# pull data for the new dates
source("read_config.r")
source("get_fs_tseries.r")

update_tseries <- function(config_file) {
    config <- read_config(config_file)
    # moving to local variables.  throws error if the accessed parameter does not exist in the file.
    output_output_prefix <- config$OUTPUT_PREFIX
    output_dir <- config$OUTPUT_DIR
    params_file <- config$PARAMS_FILE
    curr <- config$CURRENCY
    t1 <- config$T1
    if( t1 <= 0 ) {
        t1 = Sys.Date() + t1
    }
    
    # Query time out.  Default is 120 secs.  120-3600 secs on client side.
    FactSet.setConfigurationItem( FactSet.TIMEOUT, 900 )
    
    file_list <- list.files(output_dir, pattern=paste("^",output_prefix, ".+", curr, ".*", "csv$", sep=""))
    # Read the FS function list that comes with frequency
    para_list <- read.table(params_file, sep="=", comment.char="#", strip.white=TRUE, as.is=TRUE)
    rownames(para_list) <- para_list[,1] # fs parameter names
    para_list[,1] <- c()
    
    for( filename in file_list[1] ){
        tokens <- unlist(strsplit(sub("^-+", "", sub(output_prefix, "", filename)), "-"))
        
        para <- tokens[1]
        freq <- tokens[2]
        
        # skip ones that are commented
        if( length(para_list[para,]) < 1 ) {
            print(paste(para, " appears commented in the paramater list.  Skipping..."))
            break
        }
        
        input_fullname <- file.path(output_dir, filename)
        fin <- file(input_fullname, 'r')
        ids <- read.table(fin, sep=",", nrows=1, colClasses="character")
        ids <- ids[2:length(ids)]
    
        #print(paste(ids, collapse=","))
        last_line <- ""
        wc=0
        while( TRUE ){
            line <- readLines(fin, n=1)
            if( length(line) < 1 ) break
            wc <- wc+1
            last_line <- line
        }
        close(fin)
        tokens <- unlist(strsplit(last_line, ","))
        last_date <- as.Date(tokens[1], format="%m/%d/%Y")
        
        t0 <- last_date + 1
        
        print(paste("Time frame:", t0, "to", t1))
        
        stem <- sub(".csv", "", filename, fixed=TRUE)[1]
        output_filename <- paste(stem, "-patch-", gsub("-", "", t0), "-", gsub("-", "", t1), ".csv", sep="")
        
        started <- proc.time()
        
        patch <- get_fs_tseries(para, ids, t0, t1,freq,curr, length(ids), output_dir, output_prefix, input_fullname) 
        output_fullpath <- file.path(output_dir, output_filename)
        write.zoo(patch, output_fullpath, sep=",")
        
        finished <- proc.time()
        
        print(paste("Output stored in: ", output_filename, sep=""))
        msg <- paste("Elapsed time for market=<", output_prefix, "> param=<", para, "> (", curr, "): ", (finished-started)["elapsed"], "s", sep="")
        
        append_updates(input_fullpath, output_fullpath)
    }
}
append_updates(target_filename, patch_filename){
    target <- file(target_filename, "a")
    patch <- file(patch_filename, "r")
    header <- readLines(patch, n=1)
    while(TRUE){
        line <- readLines(patch, n=1)
        if( length(line) < 1 ) break
        writeLines(line, target)
    }
    close(target)
    close(patch)
}
update_tseries("D:/home/honda/mpg/frontier/download_fs_tseries-cap-usd.conf")
