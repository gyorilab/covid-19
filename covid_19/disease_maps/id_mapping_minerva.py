"""This script relies on the Minerva client to get all the entities
for a given project, map them to INDRA-compatible db_refs, and standardize,
and prioritize them to find a unique grounding key that allows looking
up INDRA statements for the given entity."""
import csv
from indra.sources.minerva.minerva_client import default_map_name, \
    get_all_valid_element_refs
from indra.sources.minerva.id_mapping import indra_db_refs_from_minerva_refs
from indra.statements.agent import default_ns_order
# NOTE: this requires using the ontology_graph branch of INDRA

from indra.ontology.standardize import get_standard_name
#from indra.preassembler.grounding_mapper.standardize import \
#    standardize_db_refs, name_from_grounding



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
    version = 'v2'
    keys = get_unique_prioritized_keys(default_map_name)
    rows = []
    for db_ns, db_id in sorted(keys):
        name = get_standard_name({db_ns: db_id})
        rows.append([db_ns, db_id, '' if name is None else name])
    with open(f'minerva_disease_map_indra_ids_{version}.csv', 'w') as fh:
        writer = csv.writer(fh)
        writer.writerows(rows)
