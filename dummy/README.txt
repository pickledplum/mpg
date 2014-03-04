There is a demo script in the CD:
 * test.getCompanyInfo.r
   This script picks the list of companies that have more than $300M in working cap as of 2013.  
   Then, shows their company info (names, sector, etc.), and get the time series.
   To run the demo script, you must have the RSQLite R-plugin installed into your R installation.
   Brett or I can help you on this.
   
Without R, you can simply browse the contents of the database.
I have included a db browser called SQLiteExpert in the CD.
  
There is no Matlab APIs (yet).  I am sure Matlab would have a plugin of sort for SQLite.  
The underlying query language is the same for any clients such as Matlab or R.
You can find the queries I'm composing and sending to the database system in:

getUniverse.r
getCompanyInfo.r



