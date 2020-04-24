# Woql demo using in-memory raw data

The demo is a revision of the "vanilla" `family-tree` example,  to use in-memory data structures as raw data to initialise the database,  rather than reading raw data from an external .csv file.

Like the original `family-tree`,  it uses a simple schema of a family tree, to show composition of complex queries from simpler subqueries.  Each subquery is a Python function.

This style of Woql usage is reminiscent of Prolog clauses.

## Log output
The log output from the demo is [here](https://github.com/Chrisjhorn/terminusDB/blob/master/family-tree/family_2_ss.png) - download it to see the full .png file.
