INDRA applications and models for COVID-19
==========================================

<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/lsp_logo.png" height="30"/>
<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/hits_logo.png" height="30"/>
<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/hms_logo.png" height="30"/>

<br/>

<img align="left" src="https://raw.githubusercontent.com/sorgerlab/indra/master/doc/indra_logo.png" width="134" height="100" />
INDRA integrates multiple text-mining systems and pathway databases to
automatically extract mechanistic knowledge from the biomedical literature and
through a process of knowledge assembly, build
executable models and causal networks. Based on profiling and perturbational
data, these models can be contextualized to be cell-type specific and used to
explain experimental observations or to make predictions.

In the context of the ongoing COVID-19 pandemic, the
[`INDRA team`](https://indralab.github.io/) at the
[`Laboratory of Systems Pharmacology, Harvard Medical School`](https://hits.harvard.edu/)
is working on understanding the mechanisms by which SARS-CoV-2 infects
cells and the subsequent host response process, with the goal
of finding new therapeutics using INDRA.

- [Results](#results)
- [Integrations and collaborations](#integrations-and-collaborations)
- [General technologies for COVID-19](#general-technologies-for-covid-19)
- [Funding](#funding)


Results
=======

A self-updating model of COVID-19 literature
--------------------------------------------

<img align="left" src="https://s3.amazonaws.com/emmaa/models/covid19/covid19_image.png" width="150" />

EMMAA (Ecosystem of Machine-maintained Models with Automated Analysis) makes
available a set of computational models that are kept up-to-date using
automated machine reading, knowledge-assembly, and model generation,
integrating new discoveries immediately as they become available.

The [`EMMAA COVID-19 model`](https://emmaa.indra.bio/dashboard/covid19)
integrates all literature made available under the
[`COVID-19 Open Research Dataset Challenge (CORD-19)`](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge) and combines it with newly appearing
papers from PubMed (about 300 every day) as well as bioRxiv and
medRxiv preprints. It also integrates content from CTD, DrugBank, VirHostNet,
and many other pathway databases.

The model is also used to construct casual, mechanistic explanations to around
2,800 drug-virus effects:
- Explanations for drug-virus effects from the
[`MITRE COVID-19 Therapeutic Information Browser`](https://covidtib.c19hcc.org/)
available [`here`](https://emmaa.indra.bio/dashboard/covid19?tab=tests&test_corpus=covid19_mitre_tests).
- Explanations for drug-virus effects curated from papers describing
drug-response experiments available [`here`](https://emmaa.indra.bio/dashboard/covid19?tab=tests&test_corpus=covid19_curated_tests).

INDRA aligned with the COVID-19 Disease Map
-------------------------------------------

<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/disease_map.png" width="150"/>
The [`COVID-19 Disease Map`](https://www.nature.com/articles/s41597-020-0477-8)
brings together top pathway curators and modelers from around the world
to create a set of models to elucidate the molecular mechanisms behind
COVID-19.

We used INDRA statements assembled from all available biomedical
literature and a multitude of pathway databases to find evidence
for all interactions in the COVID-19 Disease Map, and to suggest other
mechanisms that haven't yet been included. The results are available
[`here`](https://github.com/indralab/covid-19/tree/master/covid_19/disease_maps).

We also used our [`Gilda`](https://github.com/indralab/gilda) system to find
appropriate grounding (database
identifiers) to ungrounded entities used in the Disease Map. The results of
this are available
[`here`](https://github.com/indralab/covid-19/tree/master/covid_19/disease_maps/grounding).

Reports on drugs affecting targets relevant for COVID-19
--------------------------------------------------------

<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/ace2_small_molecules.png" width="150"/>
We used INDRA to assemble all known small molecules that can inhibt a set of
protein targets that are of particular interest in treating COVID-19.
These reports are organized as browseable web pages that allow drilling down
into specific literature evidence, linking to supporting publications, and
curating any incorrect relationships. The target-specific reports are available
here: 
[`ACE2`](https://indra-covid19.s3.amazonaws.com/drugs_for_target/ACE2.html)
[`TMPRSS2`](https://indra-covid19.s3.amazonaws.com/drugs_for_target/TMPRSS2.html)
[`CTSB`](https://indra-covid19.s3.amazonaws.com/drugs_for_target/CTSB.html)
[`CTSL`]((https://indra-covid19.s3.amazonaws.com/drugs_for_target/CTSL.html))
[`FURIN`](https://indra-covid19.s3.amazonaws.com/drugs_for_target/FURIN.html).

CORD-19 documents prioritized for curators
------------------------------------------

<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/cord19_ranking_screenshot.png" width="150"/>
To support the COVID-19 Disease Map curator community, we generated a ranking
of articles in the CORD-19 corpus by the amount of molecular mechanistic
information they were likely to contain. For each article, the dataset lists 1)
the total number of mechanistic events extracted by all NLP systems supported by
INDRA, 2) the number of *unique* events extracted from the document, and
3) the number of unique events where subject and object were both molecular
entities (i.e., protein or chemical). Because the CORD-19 corpus contains
many documents that are not directly relevant to coronavirus biology, we
also generated rankings for the subset of documents tagged with the MESH
term for "coronavirus" in PubMed (MESH ID D017934). The datasets are available
at the links below:
[`All CORD-19 articles`](https://indra-covid19.s3.amazonaws.com/covid_docs_ranked_all.csv)
[`Coronavirus articles only`](https://indra-covid19.s3.amazonaws.com/covid_docs_ranked_corona.csv)

<br/>

Semantic search over INDRA COVID-19 results
-------------------------------------------

<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/semviz.png" width="150"/>
Another interface for browsing INDRA COVID-19 literature assembly results
is available via [`semviz.org`](https://www.semviz.org/)
on [`this page`](http://morbius.cs-i.brandeis.edu:23762/login?next=%2Fapp%2Fkibana#/dashboard/2b613e90-7cf0-11ea-8a44-496b85e05ba5) (login: semvizuser/semviz), an approach to semantic
browsing of biomedical relations developed at [`Brandeis University`](https://brandeis-llc.github.io/).
A tutorial video of using this interface with INDRA results to construct
hypotheses about COVID-19 is available [`here`](http://www.voxicon.net/wp-content/uploads/2020/06/semviz.mp4).

Integrations and collaborations
===============================

CoronaWhy
---------

<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/coronawhy.png" width="150"/>

CoronaWhy is a globally distributed, volunteer-powered research organisation,
assisting the medical community’s ability to answer key questions
related to COVID-19.

INDRA is a key part of the [`CoronaWhy software infrastructure`](https://www.coronawhy.org/services)
as an entrypoint to access multiple text-mining systems and pathway databases
and assembling causal models from these soures.

COVIDminer
----------

<img align="left" src="https://raw.githubusercontent.com/indralab/covid-19/website/docs/covidminer.png" width="150"/>
INDRA coupled to Reach serves as the back-end for the
[`COVIDminer`](https://rupertoverall.net/covidminer/) application developed by
[`Rupert Overall`](https://rupertoverall.net/). COVIDminer allows searching for
entities of interest for COVID-19 and visualizing the set of interactions in
their neighborhood as a graph. By clicking on graph nodes or edges, users can
learn more about each entity as well as the supporting publication and the
specific sentence serving as evidence for relations.

General technologies for COVID-19
=================================
We have developed several applications that are generally applicable to
biomedical research and can therefore also be used to study COVID-19.

- [`INDRA`](https://www.indra.bio): INDRA can be used as a [`Python package`](https://github.com/sorgerlab/indra)
  or a [`web service`](http://api.indra.bio:8000) to collect relevant
  information from the literature and pathway databases and build custom
  COVID-19 models.
- [`INDRA database`](https://db.indra.bio): The INDRA database website provides
  a search interface to find INDRA Statements assembled from the biomedical
  literature, browse their supporting evidence, and curate any errors. An
  example search relevant to COVID-19 is Object: TMPRSS2 to find entities that
  regulate the TMPRSS2 protease, which is crucial for SARS-CoV-2 entry into
  human cells.
- [`INDRA network search`](https://network.indra.bio): The INDRA network search
  allows finding causal paths, shared regulators, and common targets between
  two entities. An example search relevant to COVID-19 is Subject: ACE2,
  Object: MTOR (see [`here`](https://network.indra.bio/query?query=2495855313)).
- [`Dialogue.bio`](http://dialogue.bio): The dialogue.bio website allows
  launching dedicated human-machine dialogue sessions where you can upload your
  data (e.g., DE gene lists or gene expression profiles), discuss relevant
  mechanisms, and build model hypotheses using simple English dialogue.
  For instance, you could try the following series of questions:
  "what is ACE2?", "what does it regulate?",
  "which of those are transcription factors?".
- CLARE is a machine assistant that can be installed in any Slack workspace as
  an application. It supports direct messages or messages in channels to
  conduct dialogues about biological mechanisms. See demo video [`here`](http://sorger.med.harvard.edu/data/bgyori/clare_intro.mov). It is currently deployed in multiple workspaces
  and has answered hundreds of questions from COVID-19 researchers since
  the pandemic began. Please [`contact us`](benjamin_gyori@hms.harvard.edu)
  if you would like to install CLARE in your Slack workspace.

Funding
-------
This work is funded under the DARPA Communicating with Computers
(W911NF-15-1-0544), DARPA Automating Scientific Knowledge Extraction
(HR00111990009) and DARPA Automated Scientific Discovery Framework
(W911NF-18-1-0124) programs.
