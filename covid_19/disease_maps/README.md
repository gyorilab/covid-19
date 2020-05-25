INDRA Statements aligned with Covid-19 Disease Maps
===================================================

The `get_indra_statements.py` script uses a list of entities in the disease
maps obtained from Minerva to find relevant INDRA Statements. There are two
dumps of these statements: a `full` one which contains all INDRA Statements
that contain at least one of the disease maps entities, and a `filtered`
one which only contains INDRA Statements whose entities are all in the
disease maps.

Availability
~~~~~~~~~~~~

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

How is this different from the Covid-19 EMMAA model?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The model at https://emmaa.indra.bio/dashboard/covid19?tab=model is also
built from INDRA Statements but it is (largely) based on the content
of papers from the CORD-19 corpus of documents and isn't derived from entities
appearing in the Disease Maps. Therefore this dump which is based on entities
in the Disease Maps is more relevant for this particular use case.

Method
------
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

Using INDRA Statements
----------------------

Documentation and schema
~~~~~~~~~~~~~~~~~~~~~~~~
INDRA Statements are documented at https://indra.readthedocs.io/en/latest/modules/statements.html
and the schema for the JSON export of INDRA Statements is available at:
https://github.com/sorgerlab/indra/blob/master/indra/resources/statements_schema.json

Assembly into models and networks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If working with INDRA Statements directly is not the goal, INDRA has assemblers
for many network and model formalisms (SBML, SBGN, PySB, BNGL, Kappa, SIF,
CX, etc.) that can be invoked using INDRA as a Python package or as a
REST service.

Basics of what is in a Statement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
