"""This minimal script runs the Eidos CLI to read the CORD19 set of papers
(extracted into text files) and puts the JSON-LD outputs into a folder."""

from indra.sources.eidos import cli

cli.extract_from_directory('/pmc/covid-19/cord19_text_to_do',
                           '/pmc/covid-19/eidos_output')
