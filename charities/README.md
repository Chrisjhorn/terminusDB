# Woql demo using Irish Charities 

The demo is basically an example of an M:N relationship: each of M charities can have N trustees,  each of whom was appointed on a specific date.

The base data is publicly available at the [Irish Charities Regulator](https://www.charitiesregulator.ie/en/information-for-the-public/search-the-register-of-charities).  Although the individual names of Irish charity trustees are published by the Irish Charities Regulator at this web site,  for the demo these trustee names have been obfuscated in the form `T<number>`.

The full dataset contains some 20,000 charities.  For the purposes of the demo,  a random subset of 500 appointments have been used,  purely so that the loading time (of the raw data into the graph base) does not take too long.

## Raw data
The raw data for the demo is the `quads.csv` file in the `raw` folder.  As usual with TerminusDB,  you have to tell the server where your application data files are,  by setting the `TERMINUS_LOCAL` environment variable before starting the server.

## Log output
The log output from the demo is [here](https://github.com/Chrisjhorn/terminusDB/blob/master/charities/charities_sshot.png)

## Plot
The demo uses the standard [networkx](https://networkx.github.io/) module to plot an example sub-network.  It in turn also uses [matplotlib](https://matplotlib.org/) and [matplotlib.pyplot](https://matplotlib.org/3.2.1/api/_as_gen/matplotlib.pyplot.html).

The example is to find all of the charities,  and all of the trustees of those charities,  reachable from a given "seed" charity.  The plot produced by demo is: ![plot](https://github.com/Chrisjhorn/terminusDB/blob/master/charities/charities.png)

