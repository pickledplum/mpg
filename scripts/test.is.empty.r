source("is.empty.r")
source("assert.r")

test.is.empty <- function(){
    assert.true(is.empty(NULL))
    assert.true(is.empty(data.frame()))
    assert.true(is.empty(c()))
    assert.true(is.empty(vector()))
    assert.true(is.empty(""))
}
test.is.empty()