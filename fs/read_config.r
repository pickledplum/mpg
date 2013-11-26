# Read a config file, which is in the form of named list,
# and return a lis
read_config <- function( config_file ) {
    table <- read.table(config_file, sep="=", comment.char="#", strip.white=TRUE, as.is=TRUE)
    rownames(table) <- table[,1]
    env <- new.env(hash=TRUE)

    for( para in rownames(table) ) {
        val <- gsub(" ", "", table[para,2])
        tryCatch({
            as.numeric(val)
            assign(para, as.numeric(val), envir=env)
        }, warning = function(w){
            assign(para, val, envir=env)
        })
    }
    return(as.list(env))
}