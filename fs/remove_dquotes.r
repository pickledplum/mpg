# Remove double-quotes
source_dir_list <- c("Z:/Backups/MPG-Production/fs-outputs/acwi",
                     "Z:/Backups/MPG-Production/fs-outputs/fm",
                     "Z:/Backups/MPG-Production/fs-outputs/em",
                     "Z:/Backups/MPG-Production/fs-outputs/dm")

target_dir_list <- c("D:/home/honda/mpg/acwi/fs_output",
                     "D:/home/honda/mpg/frontier/fs_output",
                     "D:/home/honda/mpg/emerging/fs_output",
                     "D:/home/honda/mpg/developed/fs_output")

hash <- new.env(hash=TRUE)
for( i in 1:length(source_dir_list)){
    assign( source_dir_list[i], target_dir_list[i], hash)
}
ls(hash)


for( source_dir in source_dir_list ){
    target_dir <- hash[[source_dir]]
    
    source_names <- list.files(source_dir, pattern=paste(".csv$", sep=""), full.names=FALSE)
    
    for( filename in source_names ){
        print(paste("opening", filename, "..."))
        
        # count the number of rows
        fin <- file(file.path(source_dir, filename), "r")
        header <- readLines(fin, n=1)
        n <- 0
        while(TRUE){
            line <- readLines(fin,1)
            if( length(line) < 1 ) break
            n <- n+1
        }
        close(fin)



        fin <- file(file.path(source_dir, filename), "r")
        line <- readLines(fin,1) # ignore the header line
        
        fout <- file(file.path(target_dir, filename), "w") 
        new_header <- gsub("Index", n, header, ignore.case=TRUE)
        writeLines(new_header, fout)
        
        while(TRUE){
            line <- readLines(fin,1)
            if( length(line) < 1 ) break
            writeLines(line, fout)
        }
        close(fin)
        close(fout)
    }
}