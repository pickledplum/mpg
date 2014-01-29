library(FactSetOnDemand)
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
tryExtract <- function(fql_map, param, id, d1, d2, freq, curr, max_trials=5) {
    for( i in seq(1,max_trials) ){
        tryCatch({
            formula <- fql_map[param, ]$syntax
            formula <- gsub("<d1>",d1,formula)
            formula <- gsub("<d2>",d2,formula)
            formula <- gsub("<freq>",freq,formula)
            formula <- gsub("<curr>",curr,formula)
            formula <- gsub("<ID>", id, formula)
            print(formula)
            ret <- eval(parse(text=formula))
            return(ret)
        }, error=function(msg){
            if( i <= max_trials ){
                logger.warn(paste(i, "th trials:", msg))
                Sys.sleep(1)
                        
            } else{
                logger.error(paste("Max number (", max_trials, ") of trials failed:", msg))
                return(data.frame())
            }
        })
    }
}
