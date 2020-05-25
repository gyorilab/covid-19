# INDRA Statements aligned with Covid-19 Disease Maps

The `get_indra_statements.py` script uses a list of entities in the
COVID-19 Disease
Maps (https://covid19map.elixir-luxembourg.org/minerva/) obtained
through the Minerva API to find relevant INDRA Statements. There are two
dumps of these statements: a `full` one which contains all INDRA Statements
that contain _at least one_ of the disease maps entities (but can contain
other entities as well), and a `filtered`
one which only contains INDRA Statements whose entities are _all_
in the disease maps.

## Availability

The query for all entities yields a total of 656,418 INDRA Statements.
These statements are available as a gzipped JSON dump at

- https://indra-covid19.s3.amazonaws.com/disease_maps/disease_map_indra_stmts_full.json.gz

and as a Python pickle file (which can be deserialized into INDRA
Statement objects directly) at

- https://indra-covid19.s3.amazonaws.com/disease_maps/disease_map_indra_stmts_full.pkl

The filtered corpus contains 40,361 INDRA Statements.
These statements are available as a gzipped JSON dump at

- https://indra-covid19.s3.amazonaws.com/disease_maps/disease_map_indra_stmts_filtered.json.gz

and as a Python pickle file (which can be deserialized into INDRA
Statement objects directly) at

- https://indra-covid19.s3.amazonaws.com/disease_maps/disease_map_indra_stmts_filtered.pkl

### How else can I access the same INDRA content?
If you are looking for a more targeted way to find INDRA content, you can
- Submit specific queries on the INDRA Database website: https://db.indra.bio/statements
or REST API
- Ask a question from the CLARE system integrated into the Disease Maps Slack
workspace. See some examples in http://sorger.med.harvard.edu/data/bgyori/clare_intro.mov.

### How is this different from the Covid-19 EMMAA model?
The model at https://emmaa.indra.bio/dashboard/covid19?tab=model is also
built from INDRA Statements but it is (largely) based on the content
of papers from the CORD-19 corpus (https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge)
of documents and isn't derived from entities
appearing in the Disease Maps. Therefore this dump which is based on entities
in the Disease Maps is more relevant for this particular use case.

## Plan for next update
By May 31st we are planning to update the content of these models with a more recent
build of the INDRA Database which will contain content from:
- More recent publications and preprints relevant to Covid-19 specifically
- More full-text publications in the CORD-19 corpus
- Extended drug-target interactions
- Queries for entities that are not yet fully identified in the Disease Maps (see analysis at https://github.com/indralab/covid-19/tree/master/covid_19/disease_maps/grounding which attempts to extend these identifiers.

## Method
The `id_mapping_minerva.py` script is used to get all the elements of
the Disease Map models and find their IDs which are best aligned with
ones that INDRA uses internally. This produces a list of IDs that are
exported into `diseasemap_indra_mappings.csv`.

Some of these entities such as ATP, UTP, oxygen, phosphate, etc. are generic
by-products of biochemical reactions. Thos INDRA collects a very large number
of statements about these, they are not immediately of interest for discovering
new knowledge, and therefore we do not query for them (they are listed
in the `black_list` variable in `get_indra_statements.py`.

Each entity is then the basis of a query into the INDRA Database to find all
INDRA Statements in which the entity appears to create the `full` corpus.
The `filtered` corpus is obtained by filtering for Statements in which
every Agent is also a disease maps entity.

## Using INDRA Statements

### Documentation and schema
INDRA Statements are documented at https://indra.readthedocs.io/en/latest/modules/statements.html
and the schema for the JSON export of INDRA Statements is available at:
https://github.com/sorgerlab/indra/blob/master/indra/resources/statements_schema.json

### Browsing and curating Statements
Each Statement in the JSON dump has a `matches_hash` attribute which
allows linking to it in the INDRA Database web service.
An example is https://db.indra.bio/statements/from_hash/8173962799466177?format=html
where `8173962799466177` is the `matches_hash` of a statement.

This INDRA browser page allows:
- Reading the evidence sentences supporting the statement
- Linking out to entity databases by clicking on the name of the agents in the statement
- Linking out to publications from which the evidences were extracted
- Curating statements as correct or incorrect (see guide at 
https://indra.readthedocs.io/en/latest/tutorials/html_curation.html)

### Assembly into models and networks
If working with INDRA Statements directly is not the goal, INDRA has assemblers
for many network and model formalisms (SBML, SBGN, PySB, BNGL, Kappa, SIF,
CX, etc.) that can be invoked using INDRA as a Python package or as a
REST service. For a full list with links to documentation, see
https://github.com/sorgerlab/indra#output-model-assemblers.

### Basics of what is in a Statement
Each Statement has a type (e.g., Phosphorylation or Activation) and has one
or more Agent arguments depending on the type. Each Agent is an entity which
has a name (typically standardized like an HGNC symbol) and a set of
groundings in `db_refs` with various database identifiers. The `TEXT` entry
in `db_refs` is the original entity text as it appeared in text.

Each
Statement also has one or more Evidences associated with it. Evidences
contain the sentence from the literature from which the Statement was
extracted, the PMID or other text references (DOI, etc.) of the publication,
the source API via which the Statement was obtained (reach, sparser, etc.) and
many other annotations.

Each Statement also comes with a `belief` score
which is based on an error model of each source and the amount of evidence
for a given statement from each source. It can be interpreted as a predicted
probability that a given Statement is not incorrect.
