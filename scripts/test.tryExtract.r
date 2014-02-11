source("tryExtract.r")
source("enQuote.r")

fql <- "FF_ASSETS"
fql_syntax <- "FF.ExtractFormulaHistory('<ID>', 'FF_WKCAP', '<d1>:<d2>:<freq>','curr=<curr>')"
id_list <- (c("GOOG", "00101J10", "JP3902400005", "B0L4T2"))
d1 <- "19800101"
d2 <- "20131231"
freq <- "Y"
curr <- "usd"
tryBulkExtract(fql_syntax, id_list, d1, d2, freq, curr)