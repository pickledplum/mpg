extractDirFromPath <- function( path ){

    tokens <- strsplit(path, "/")
    leaf <- tokens[[1]][length(tokens[[1]])]
    dir <- regmatches(path, regexpr(leaf, path), invert=TRUE)[[1]][1]
    dir <- sub("/$", "", dir)  # remove trailing path-separator if any
    return(dir)
}
extractDBDirFromPath <- function( conn ){

    return(extractDirFromPath(dbGetInfo(conn)$dbname))
}


