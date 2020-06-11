"""This script maps identifiers URLs of entities based on the SBGN export of
the disease maps by Augustin Luna to INDRA-compatible IDs, and then finds
entities in the Covid-19 EMMAA model that match these entities."""
import json
import pandas
import requests
from collections import defaultdict
from indra.statements import stmts_from_json
from indra.ontology.standardize import standardize_db_refs
from indra.databases import get_identifiers_url, hgnc_client
from emmaa.util import find_latest_s3_file, get_s3_client


def get_bigg_chebi_mappings():
    df = pandas.read_csv(
        'http://bigg.ucsd.edu/static/namespace/bigg_models_metabolites.txt',
        sep='\t')
    mappings = defaultdict(list)
    for _, row in df.iterrows():
        if pandas.isna(row['database_links']):
            continue
        links = row['database_links'].split('; ')
        for link in links:
            if link.startswith('CHEBI'):
                chebi_id = link[6:].rsplit('/')[-1]
                mappings[row['bigg_id']].append(chebi_id)
    return dict(mappings)


def get_groundings_set_from_indra_statements(stmts):
    indra_groundings = set()
    for stmt in stmts:
        for agent in stmt.agent_list():
            if agent is None:
                continue
            for k, v in agent.db_refs.items():
                if k not in {'TEXT', 'TEXT_NORM'}:
                    indra_groundings.add((k, v))
    return indra_groundings


def get_emmaa_model_statements(model):
    s3 = get_s3_client(unsigned=True)
    latest_model_json = find_latest_s3_file('emmaa', 'assembled/%s' % model)
    stmts_obj = s3.get_object(Bucket='emmaa', Key=latest_model_json)
    stmts = stmts_from_json(json.loads(stmts_obj['Body'].read().decode('utf-8')))
    return stmts


def get_disease_maps_urls_cannin():
    # Read disease maps URLs
    url = ('https://raw.githubusercontent.com/cannin/covid19-analysis/'
           'master/disease_maps_ids.txt')
    dm_urls = requests.get(url).text.split('\n')[:-1]
    return dm_urls


def align_identifiers_urls(indra_groundings, dm_urls):
    matches = []
    identifiers_prefix = 'https://identifiers.org/'
    for dm_url in dm_urls:
        # We do it this way instead of splitting because of DOIs which have
        # extra slashes
        entity = dm_url[len(identifiers_prefix):]
        db_ns, db_id = entity.split(':', maxsplit=1)
        if db_ns == 'CHEBI':
            db_refs = [standardize_db_refs({'CHEBI': '%s:%s' % (db_ns, db_id)})]
        elif db_ns == 'hgnc':
            db_refs = [standardize_db_refs({'HGNC': db_id})]
        elif db_ns == 'hgnc.symbol':
            hgnc_id = hgnc_client.get_current_hgnc_id(db_id)
            db_refs = [standardize_db_refs({'HGNC': hgnc_id})]
        elif db_ns == 'pubchem.compound':
            db_refs = [standardize_db_refs({'PUBCHEM': db_id})]
        elif db_ns == 'uniprot':
            db_refs = [standardize_db_refs({'UP': db_id})]
        elif db_ns == 'bigg.metabolite':
            chebi_ids = bigg_to_chebi.get(db_id)
            if chebi_ids:
                db_refs = [standardize_db_refs({'CHEBI': chebi_id})
                           for chebi_id in chebi_ids]
            else:
                db_refs = [{}]
        elif db_ns == 'ncbigene':
            hgnc_id = hgnc_client.get_hgnc_from_entrez(db_id)
            if hgnc_id:
                db_refs = [standardize_db_refs({'HGNC': hgnc_id})]
            else:
                db_refs = [{}]
        # Skip literature references that aren't entities
        elif db_ns in {'doi', 'pubmed'}:
            continue
        else:
            print('Unhandled namespace %s' % db_ns)
            db_refs = {}

        matched = None
        for db_ref in db_refs:
            for k, v in db_ref.items():
                if (k, v) in indra_groundings:
                    matched = (k, v)
                    break

        matches.append((dm_url,
                        get_identifiers_url(*matched) if matched else None))
    return matches


if __name__ == '__main__':
    bigg_to_chebi = get_bigg_chebi_mappings()
    # Get INDRA statements from latest EMMAA model
    stmts = get_emmaa_model_statements('covid-19')
    indra_groundings = get_groundings_set_from_indra_statements(stmts)
    # Get disease maps identifiers URLs
    dm_urls = get_disease_maps_urls_cannin()
    # Align the two
    matches = align_identifiers_urls(indra_groundings, dm_urls)
    # Dump the results
    with open('diseasemap_indra_mappings.csv', 'w') as fh:
        for m1, m2 in matches:
            fh.write('%s,%s\n' % (m1, m2 if m2 else ''))
