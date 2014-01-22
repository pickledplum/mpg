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

tryExtract <- function(param, d1, d2, freq, curr) {
    formula <- fql_map[param, ]$fql
    formula <- gsub("<d1>",d1,formula)
    formula <- gsub("<d2>",d2,formula)
    formula <- gsub("<freq>",freq,formula)
    formula <- gsub("<curr>",curr,formula)
    formula <- gsub("<ID>", id, formula)
    print(formula)
    ret <- eval(parse(text=formula))
    return(ret)
}
