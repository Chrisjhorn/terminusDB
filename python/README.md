# TerminusDB
Some contributed tutorial and example software in Python to the [TerminusDB](https://terminusdb.com/) open source graph base.

## Tutorials
The tutorials each are a [Jupyter Notebook](https://jupyter.org/),  so that you can interactively run the Python yourself as you read through the text.

If you don't have Jupyter,  then there is also a `.html` file containing the full tutorial, including the results of running each code segment. Git may not display the .html file directly, so download it and then use your browser to view the download.

I hope in due course to do a series of tutorials.  Right now,  there is just:
* `Tutorial 1` - getting started with TerminusDB
* `Tutorial 2` - loading .csv files (locally, or over the internet) into TerminusDB, and cleaning up the raw data

## Examples
So far,  I only have four:
* `charities` -- an example of an M:N relationship schema,  using public data about registered Irish charities.
* `family-tree` -- an example of using composable subqueries,  using an external .csv file as raw data
* `family-tree-2` -- another version of `family-tree` which uses in-memory structures to initialise the database,  rather than an external .csv file
* `shipping` -- an animation of maritime traffic at Dublin port, using ephemeral events
