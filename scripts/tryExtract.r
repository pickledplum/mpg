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

#' returns the list of data frames, each containing the t-series for a company
#' list[[1]]
#'Id    Date            val
#' A     2011-01-01      0.8
#' A     2011-01-02      4.0
#' A     2011-01-03      2.3
#' ...
#'
#' list[[2]]
#' A     2011-12-31
#' B     2011-01-01      32.4
#' B     2011-01-03      NA
#' B     2011-01-04      28.0
#' ...
#' B     2011-12-15      12.2
#'
tryBulkExtract <- function(fql_syntax, id_list, d1, d2, freq, curr, max_failures=5) {
    
    data <- NULL
    data_list <- list(rep(data.frame(), length(id_list)))
    for( nfailure in seq(1, max_failures)) {
        tryCatch({
            formula <- fql_syntax
            formula <- gsub("<d1>",d1,formula)
            formula <- gsub("<d2>",d2,formula)
            formula <- gsub("<freq>",freq,formula)
            formula <- gsub("<curr>",curr,formula)
            formula <- gsub("<ID>", paste(id_list, collapse=","), formula)
            logger.debug(paste("FactSet formula:", formula))
            # this is a data frame containing all companies data vertically.
            #
            # Id    Date            val
            # A     2011-01-01      0.8
            # A     2011-01-02      4.0
            # A     2011-01-03      2.3
            # ...
            # A     2011-12-31
            # B     2011-01-01      32.4
            # B     2011-01-03      NA
            # B     2011-01-04      28.0
            # ...
            # B     2011-12-15      12.2
            #
            # NOTE that there may be NAs and the number of data points per company may well vary.
            ret <- eval(parse(text=formula))
            if( is.empty(ret) ){
                break
            }
            # split by companies, into the list of data frames
            data_list <- split(ret, f=factor(ret[["Id"]]))
            
            for( i in seq(1, length(data_list)) ){
                data <- data_list[[i]]
                if( is.empty(data) ) next
                
                filter <- which(is.na(data[[3]]))
                ddata <- data[-filter,]
                #if( is.empty(ddata) ) next
                id <- data[1,1]
                colnames(ddata) <- c("Id", "Date", id)
                data_list[[i]] <- ddata
            }
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

    return(data_list)
}
trySnapshot <- function(company_id, items, max_failures=5) {
    
    ret <- NULL
    for( nfailure in seq(1, max_failures)) {
        tryCatch({
            items_str <- paste(items, collapse=",")
            
            logger.debug(items_str)
            ret <- FF.ExtractDataSnapshot(company_id, items)
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
