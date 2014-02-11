library(FactSetOnDemand)
source("logger.r")
# param <- "FF_ASSETS"
# d1 <- "20120101"
# d2 <- "20121231"
# freq <- "Q"
# curr <- "USD"
# id <- "002826"
# 
# fql_map <- read.csv("/Documents and Settings/Sachko Honda/My Documents/MPG/FactSet-items-mapping.csv")
# rownames(fql_map) <- fql_map$item
# 
# param <- fql_map["FF_DIV_YLD",]$item
# 
# ret <- tryExtract(param, d1, d2, freq, curr)
# print(ret)
#
# Return a data frame containg the data.  Empty data.frame if error occurs.
# Try max_trials times before it gives up.  Default is 5.
#
tryExtract <- function(fql_syntax, id, d1, d2, freq, curr, max_failures=5) {

    ret <- NULL
    for( nfailure in seq(1, max_failures)) {
        tryCatch({
            formula <- fql_syntax
            formula <- gsub("<d1>",d1,formula)
            formula <- gsub("<d2>",d2,formula)
            formula <- gsub("<freq>",freq,formula)
            formula <- gsub("<curr>",curr,formula)
            formula <- gsub("<ID>", id, formula)
            logger.debug(formula)
            ret <- eval(parse(text=formula))
            break
        }, error = function(msg){
            if( nfailure > max_failures){
                logger.error(paste(nfailure, "th failure:", msg, sep=""))
                stop(msg)
            }
            logger.warn(paste(nfailure, "th failure:", msg, sep=""))
            Sys.sleep(1)
        }
        )
    }
    if( is.null(ret) ) return(data.frame())
    return(ret)
}
tryBulkExtract <- function(fql_syntax, id_list, d1, d2, freq, curr, max_failures=5) {
    
    data <- NULL
    for( nfailure in seq(1, max_failures)) {
        tryCatch({
            formula <- fql_syntax
            formula <- gsub("<d1>",d1,formula)
            formula <- gsub("<d2>",d2,formula)
            formula <- gsub("<freq>",freq,formula)
            formula <- gsub("<curr>",curr,formula)
            formula <- gsub("<ID>", paste(id_list, collapse=","), formula)
            logger.debug(formula)
            ret <- eval(parse(text=formula))
            # split by companies
            data_list <- split(ret, f=factor(ret[["Id"]]))
            dates <- data_list[[1]][["Date"]]
            mat <- matrix(NA, length(dates), length(data_list))
            for( i in seq(1, length(data_list) ) ){
                vals <- data_list[[i]][[3]]
                mat[,i] <- vals
            }
            data <- data.frame(mat)
            colnames(data) <- id_list
            rownames(data) <- dates
            break
        }, error = function(msg){
            if( nfailure > max_failures){
                logger.error(paste(nfailure, "th failure:", msg, sep=""))
                stop(msg)
            }
            logger.warn(paste(nfailure, "th failure:", msg, sep=""))
            Sys.sleep(1)
        }
        )
    }

    if( is.null(data) ) return(data.frame())
    return(data)
}
