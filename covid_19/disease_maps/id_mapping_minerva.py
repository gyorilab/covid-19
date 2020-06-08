"""This script relies on the Minerva client to get all the entities
for a given project, map them to INDRA-compatible db_refs, and standardize,
and prioritize them to find a unique grounding key that allows looking
up INDRA statements for the given entity."""
from covid_19.disease_maps.minerva_client import \
    get_all_valid_element_refs, default_map_name
from indra.statements.agent import default_ns_order
# NOTE: this requires using the ontology_graph branch of INDRA
from indra.databases import chebi_client
from indra.ontology.standardize import standardize_db_refs, get_standard_name
#from indra.preassembler.grounding_mapper.standardize import \
#    standardize_db_refs, name_from_grounding


minerva_to_indra_map = {
    'UNIPROT': 'UP',
    'REFSEQ': 'REFSEQ_PROT',
    'ENTREZ': 'EGID',
    'INTERPRO': 'IP',
}


def fix_id_standards(db_ns, db_id):
    if db_ns == 'CHEBI':
        if not db_id.startswith('CHEBI:'):
            db_id = f'CHEBI:{db_id}'
        db_id = chebi_client.get_primary_id(db_id)
    elif db_ns == 'HGNC' and db_id.startswith('HGNC:'):
        db_id = db_id[5:]
    return db_ns, db_id


def indra_db_refs_from_minerva_refs(refs):
    db_refs = {}
    for db_ns, db_id in refs:
        db_ns = minerva_to_indra_map[db_ns] \
            if db_ns in minerva_to_indra_map else db_ns
        db_nbs, db_id = fix_id_standards(db_ns, db_id)
        db_refs[db_ns] = db_id
    db_refs = standardize_db_refs(db_refs)
    return db_refs


def get_prioritized_db_refs_key(db_refs):
    for db_ns in default_ns_order:
        db_id = db_refs.get(db_ns)
        if db_id:
            return db_ns, db_id
    return None, None


def get_unique_prioritized_keys(map_name=default_map_name):
    valid_element_refs = get_all_valid_element_refs(map_name)
    db_refs = [indra_db_refs_from_minerva_refs(refs)
               for refs in valid_element_refs]
    prioritized_keys = [get_prioritized_db_refs_key(db_ref)
                        for db_ref in db_refs]
    unique_prioritized_keys = {key for key in prioritized_keys
                               if key[0] is not None}
    return unique_prioritized_keys


if __name__ == '__main__':
    keys = get_unique_prioritized_keys(default_map_name)
    with open('minerva_disease_map_indra_ids.csv', 'w') as fh:
        for db_ns, db_id in sorted(keys):
            name = get_standard_name({db_ns: db_id})
            fh.write('%s,%s,%s\n' % (db_ns, db_id, name))
