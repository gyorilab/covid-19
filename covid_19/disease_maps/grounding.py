import csv
import gilda
from gilda.grounder import Term
from covid_19.process_gordon_ndex import mappings
from covid_19.disease_maps.minerva_client import default_map_name, \
    get_model_elements, get_models, get_config, \
    get_project_id_from_config


def get_ungrounded_elements(model_id, project_id, map_name=default_map_name):
    model_elements = get_model_elements(model_id, project_id, map_name)
    ungrounded = [element for element in model_elements
                   if not element.get('references')]
    return ungrounded


def resolve_complex(element):
    split_chars = [':', '_', '/']
    for split_char in split_chars:
        if split_char in element['name']:
            parts = element['name'].split(split_char)
            break
    else:
        return []
    groundings = []
    for part in parts:
        term = ground_simple_txt(part)
        groundings.append(term)
    return groundings


def ground_simple_txt(txt):
    # Try the SARS-CoV-2 protein mappings first
    refs = mappings.get(txt)
    if refs:
        return Term(norm_text=txt, text=txt,
                    db='UP', id=refs['UP'],
                    entry_name=txt, source='manual',
                    status='synonym')
    matches = gilda.ground(txt)
    if matches:
        return matches[0].term
    return None


def ground_element(element):
    txt = element['name'].replace('\n', ' ')
    term = ground_simple_txt(txt)

    if not term and element['type'] == 'Complex':
        return resolve_complex(element)
    else:
        return [term]


def dump_results(fname, groundings, models):
    header = ['model_id', 'model_name',
              'entity_id', 'entity_name', 'entity_type',
              'primary_reference_type', 'primary_reference_resource',
              'primary_standard_name',
              'all_references']
    rows = [header]
    for model in models:
        # Skip the overview model
        if model['name'] == 'overview':
            continue
        for element in groundings[model['idObject']]:
            if element['references']:
                ref_type = '/'.join(ref['type']
                                    for ref in element['references'])
                ref_resource = '/'.join(ref['resource']
                                        for ref in element['references'])
                standard_name = '/'.join(ref['name']
                                         for ref in element['references'])
            else:
                ref_type = ref_resource = standard_name = ''
            row = [model['idObject'], model['name'],
                   element['id'], element['name'], element['type'],
                   ref_type, ref_resource, standard_name, '']
            rows.append(row)
    with open(fname, 'w') as fh:
        writer = csv.writer(fh)
        writer.writerows(rows)


if __name__ == '__main__':
    config = get_config(default_map_name)
    project_id = get_project_id_from_config(config)
    models = get_models(project_id, default_map_name)
    ungrounded_per_model = {}
    for model in models:
        # Skip the overview model
        if model['name'] == 'overview':
            continue
        model_id = model['idObject']
        ungrounded_per_model[model_id] = \
            get_ungrounded_elements(model_id, project_id, default_map_name)
        for element in ungrounded_per_model[model_id]:
            grounded_terms = ground_element(element)
            for grounded_term in grounded_terms:
                element['references'].append(
                    {
                     'resource': grounded_term.id if grounded_term else '',
                     'type': grounded_term.db if grounded_term else '',
                     'name': grounded_term.entry_name if grounded_term else ''
                     }
                )
    dump_results('groundings_v1.csv', ungrounded_per_model, models)
