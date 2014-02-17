source("tryExtract.r")
source("enQuote.r")

# fql <- "FF_ASSETS"
# fql_syntax <- "FF.ExtractFormulaHistory('<ID>', 'FF_WKCAP', '<d1>:<d2>:<freq>','curr=<curr>')"
# id_list <- (c("GOOG", "00101J10", "JP3902400005", "B0L4T2"))
# d1 <- "19800101"
# d2 <- "20131231"
# freq <- "Y"
# curr <- "usd"
# tryBulkExtract(fql_syntax, id_list, d1, d2, freq, curr)
# 
# 
    universe <- c("209478")
    #universe <- c("200542", "235938", "238205")
    tryBulkExtract( 
        "FF.ExtractFormulaHistory('<ID>', 'FF_WKCAP', '<d1>:<d2>:<freq>','curr=<curr>')",
        universe,
        "19800101", "20131231", "Y", "usd")
    
    # ret <- eval(parse(text="FF.ExtractFormulaHistory('200542,235938,238205', 'FF_WKCAP', '19800101:20131231:Y','curr=usd')"))
    # data_list <- split(ret, f=factor(ret[["Id"]]))
    # 
    # for( i in seq(1, length(data_list)) ){
    #     data <- data_list[[i]]
    #     filter <- which(is.na(data[[3]]))
    #     ddata <- data[-filter,]
    #     #if( is.empty(ddata) ) next
    #     
    #     colnames(ddata) <- c("Id", "Date", id)
    #     data_list[[i]] <- ddata
    # }
    # print(data_list)

