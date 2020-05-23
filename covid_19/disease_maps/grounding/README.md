Finding missing references for disease map elements using named entity normalization (grounding)
================================================================================================
The `grounding.py` script uses the Gilda system
(https://github.com/indralab/gilda) to ground (i.e., find references for)
named entities in the Covid-19 disease maps.

Method
------
All models for the `covid19map` are fetched from Minerva via its API. Iterating
over each model, the script finds all elements in the model that do not
have any references listed. The `name` associated with the element is then
used to find references for the given element.

In the case of complexes, the components of the complex are represented
using different conventions, including `A:B:C`, `A/B/C`, and `A_B_C`. The
script handles all of these patterns, and grounds the parts individually.

Groundings automatically
found by Gilda are combined with some manually curated grounding mappings
specific to Covid-19 that the INDRA team has put together to help with
text mining in general.

Results
-------
There are a total of 885 ungrounded elements that the script runs on. Out
of this, 470 are fully grounded, and 35 complexes are partially grounded
(i.e., one or more parts are grounded but at least one part remains
ungrounded).

The results are dumped into a comma-separated value table available at
https://github.com/indralab/covid-19/blob/master/covid_19/disease_maps/grounding/groundings.csv
(the table is wide so if you're looking at it on the web, make sure
to scroll to the right to see the results).
The structure of this table is as follows: each row provides the ID and
the name of the model in which the given element appears. It also provides
the ID and name and type of the element as it appears in the model.

