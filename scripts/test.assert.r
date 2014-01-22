source("assert.r")

test.assert <- function(){
    tryCatch({
        assert.equal(1.123, 1.123)
        assert.equal(NULL,NULL)
        
        assert.equal("a", "a")
        
        assert.not.equal(1, 1.1)
        assert.not.equal(NULL, NA)
        
        
        assert.equal.reltol(1, 1.1, .1)
    }, error=function(msg){
        print(msg)
        print(paste("Unexpected assertion error!  Bad!"))
    })
    tryCatch({
        assert.equal(NULL,NA)
        #assert.equal.reltol(1, 1.1, .09)
    }, error=function(msg){
        print(paste("Expected assertion violation occurred.  Good!"))
    })
}

test.assert()