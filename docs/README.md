INDRA applications and models for COVID-19
==========================================

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

- [COVID-19 results](#results)
- [COVID-19 Integrations and collaborations](#integrations)
- [General technologies for COVID-19](#general)
- [Funding](#funding)


COVID-19 results
================

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
[`FURIN`](https://indra-covid19.s3.amazonaws.com/drugs_for_target/FURIN.html)


General technologies for COVID-19
=================================
There are several applications built on top of INDRA that that are generally
applicable to biomedical research and can therefore also be used to study
COVID-19 mechanisms.
- [`INDRA database`](https://db.indra.bio): The INDRA database website provides
  a search interface to find INDRA Statements assembled from the biomedical
  literature, browse their supporting evidence, and curate any errors.  Az
  example search relevant to COVID-19 is Object: TMPRSS2 to find entities that
  regulate the TMPRSS2 protease, which is crucial for SARS-CoV-2 entry into
  human cells.
- [`INDRA network search`](https://network.indra.bio): The INDRA network search
  allows finding causal paths, shared regulators, and common targets between
  two entities. An example search relevant to COVID-19 is Subject: bradykinin,
  Object: PKC (see [`here`](https://network.indra.bio/query?query=2094257329)).
- [`Dialogue.bio`](http://dialogue.bio): The dialogue.bio website allows
  launching dedicated human-machine dialogue sessions where you can upload your
  data (e.g., DE gene lists or gene expression profiles), discuss relevant
  mechanisms, and build model hypotheses using simple English dialogue.
  For instance, you could try the following series of questions:
  "what is Nav1.8?", "what does it regulate?",
  "which of those are transcription factors?".

Funding
-------
This project is funded under the DARPA Communicating with Computers
(W911NF-15-1-0544) and DARPA Automating Scientific Knowledge Extraction
(HR00111990009) programs.
