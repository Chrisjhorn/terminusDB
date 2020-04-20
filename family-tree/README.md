# Woql demo using composable subqueries 

The demo uses a simple schema of a family tree, to show composition of complex queries from simpler subqueries.  Each subquery is a Python function.

This style of Woql usage is reminiscent of Prolog clauses.

## Raw data
The raw data for the demo is the [`people.csv` file](https://raw.githubusercontent.com/Chrisjhorn/terminusDB/blob/master/family-tree/people.csv).  If you want to download the raw data and alter it,  then as usual with TerminusDB,  you have to tell the server where your application data files are,  by setting the `TERMINUS_LOCAL` environment variable before starting the server.

Because the TerminusDB crew are headquartered in Ireland,  I had of course to use Irish names in the family-tree :-)

## Log output
The log output from the demo is [here](https://github.com/Chrisjhorn/terminusDB/blob/master/family-tree/family_ss.png) - download it to see the full .png file.
