library(tools)
library(FactSetOnDemand)

ids <- c("002826", "004561", "00101J10")  # first two are British companies, the 3rd one is USA.
items <- c("P_PRICE_AVG", "FF_ASSETS")
freqs <- c("D", "Q")
t0 <- "20120101"
t1 <- "20121231"
currs <- c("USD", "local")
d <- data.frame(items, freqs, stringsAsFactors =FALSE)

id <- ids[1]
item <- items[2]
freq <- freqs[1]
curr <- currs[1]
# FF.ExtractFormulaHistory("002826", P_PRICE_AVG(20120101,20121231,D,USD))
res1 <- FF.ExtractFormulaHistory(as.character(id), 
                                  paste(item, "(", t0, ",", t1, ",", freq, ",", curr, ")",sep=""))
                  
# FF.ExtractFormulaHistory("002826", "P_PRICE_AVG", "20120101:20121231:D","curr=USD"))
res2 <- FF.ExtractFormulaHistory(as.character(id),
                                 item,
                                 paste(t0,t1,freq,sep=":"),
                                 paste("curr=",curr,sep=""))
