import csv
import gilda
from covid_19.disease_maps.minerva_client import default_map_name, \
    get_model_elements, get_models, get_config, \
    get_project_id_from_config


def get_ungrounded_elements(model_id, project_id, map_name=default_map_name):
    model_elements = get_model_elements(model_id, project_id, map_name)
    ungrounded = [element for element in model_elements
                   if not element.get('references')]
    return ungrounded


def ground_element(element):
    matches = gilda.ground(element['name'])
    if not matches:
        return None
    return matches[0].term


def dump_results(fname, groundings, models):
    header = ['model_id', 'model_name',
              'entity_id', 'entity_name', 'entity_type',
              'primary_reference_type', 'primary_reference_resource',
              'primary_standard_name',
              'all_references']
    rows = [header]
    for model in models:
        for element in groundings[model['idObject']]:
            if element['references']:
                primary_ref_type = element['references'][0]['type']
                primary_ref_resource = element['references'][0]['resource']
                primary_standard_name = element['references'][0]['name']
            else:
                primary_ref_type = primary_ref_resource = \
                    primary_standard_name = ''
            row = [model['idObject'], model['name'],
                   element['id'], element['name'], element['type'],
                   primary_ref_type, primary_ref_resource,
                   primary_standard_name, '']
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
        model_id = model['idObject']
        ungrounded_per_model[model_id] = \
            get_ungrounded_elements(model_id, project_id, default_map_name)
        for element in ungrounded_per_model[model_id]:
            grounded_term = ground_element(element)
            if grounded_term:
                element['references'].append(
                    {
                     'resource': grounded_term.id,
                     'type': grounded_term.db,
                     'name': grounded_term.entry_name
                     }
                )
    dump_results('groundings_v1.csv', ungrounded_per_model, models)
