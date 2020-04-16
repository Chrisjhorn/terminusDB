# Woql demo using Irish Charities 

The demo is basically an example of an M:N relationship: each of M charities can have N trustees,  each of whom was appointed on a specific date.

The base data is publicly available at the [Irish Charities Regulator](https://www.charitiesregulator.ie/en/information-for-the-public/search-the-register-of-charities).  Although the individual names of Irish charity trustees are published by the Irish Charities Regulator at this web site,  for the demo these trustee names have been obfuscated in the form "T<number".

## Plot
The demo uses the standard networkx module to plot an example sub-network.

The example is to find all of the charities,  and all of the trustees of those charities,  reachable from a given "seed" charity.  The plot produced by demo is: ![plot](https://https://github.com/Chrisjhorn/terminusDB/blob/master/charities/charities.png)
