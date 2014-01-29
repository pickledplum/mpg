library(FactSetOnDemand)
downloadUniverse <- function(portfolio_ofdb){

    data <- FF.ExtractOFDBUniverse(portfolio_ofdb, "0O")
    print(paste("Loaded universe from", portfolio_ofdb))
    universe <- data$Id
    
    return(universe)
}

outdir <- "/home/honda/mpg/frontier/output"
dir.create(outdir)
universe <- downloadUniverse(portfolio_ofdb="PERSONAL:HONDA_ALL_FM_BY_EX")
write.table(universe, file.path(outdir, "company.csv"), quote=FALSE, sep=",", row.names=FALSE, col.names=FALSE)

outdir <- "/home/honda/mpg/developed/output"
dir.create(outdir)
universe <- downloadUniverse(portfolio_ofdb="PERSONAL:HONDA_ALL_DM_BY_EX")
write.table(universe, file.path(outdir, "company.csv"), quote=FALSE, sep=",", row.names=FALSE, col.names=FALSE)

outdir <- "/home/honda/mpg/emerging/output"
dir.create(outdir)
universe <- downloadUniverse(portfolio_ofdb="PERSONAL:HONDA_ALL_EM_BY_EX")
write.table(universe, file.path(outdir, "company.csv"), quote=FALSE, sep=",", row.names=FALSE, col.names=FALSE)

outdir <- "/home/honda/mpg/acwi/output"
dir.create(outdir)
universe <- downloadUniverse(portfolio_ofdb="PERSONAL:HONDA_MSCI_ACWI_ETF")
write.table(universe, file.path(outdir, "company.csv"), quote=FALSE, sep=",", row.names=FALSE, col.names=FALSE)
