# Convert the namelist (key=val(s) pairs) to an environment.
# 
# An example of a namelist file.  The namelist can contain #-led comment lines.
#
# OUTPUT_DIR = "D:/home/honda/mpg/dummy/fs_output"
# PORTFOLIO_OFDB = "PERSONAL:HONDA_MSCI_ACWI_ETF"
# #PORTFOLIO_LIST = "list_of_constituents.txt"  # this line will be ignored.
# PREFIX = "dummy"
# T0 = "19800101"
# T1 = "0"
# DEFAULT_CURRENCY = USD
# FF_ASSETS=Q
# FF_CACH_ONLY=Q,LOCAL
#
read_config <- function( config_file ) {
    
    table <- read.table(config_file, sep="=", comment.char="#", strip.white=TRUE, as.is=TRUE)
    rownames(table) <- table[,1]
    env <- new.env(hash=TRUE)
    
    for( para in rownames(table) ) {
        val_str <- gsub(" ", "", table[para,2])
        tokens <- strsplit(val_str, ",")
        if( length(tokens[[1]]) == 1 ){
            #tryCatch({
            #    as.numeric(tokens[[1]][1])
            #    assign(para, as.numeric(tokens[[1]]), envir=env)
            #}, warning = function(w){
            #    assign(para, tokens[[1]][1], envir=env)
            #})
            assign(para, tokens[[1]][1], envir=env)
        }
        else{
            
            #l <- vector("list", length(tokens[[1]]))
            #i=1
            #for( token in tokens[[1]] ){
            #    l[i] = token
            #    tryCatch({
            #        as.numeric(token)
            #        l[i] = as.numeric(token)
            #    }, warning = function(w){
            #        #ignore
            #    })
            #    
            #    #print(l[i])
            #    i = i+1
            #}
            assign(para, unlist(tokens[[1]]), envir=env)
        }
    }
    return(env)
}

config <- read_config("/home/honda/mpg/dummy/params.conf")
#get("FF_ASSETS", config)
#exists(config)