import os
import gilda
import pandas
from collections import defaultdict
from indra.statements import Agent, Inhibition, Evidence
from indra.preassembler.grounding_mapper.standardize \
    import standardize_agent_name

here = os.path.dirname(os.path.abspath(__file__))
relations_fname = os.path.join(here, os.pardir, 'data', 'relationAllAll.csv')
evidences_fname = os.path.join(here, os.pardir, 'data', 'evidenceAllAll.csv')


virus_grounding_map = {
    'avian CoV': 'MESH:D000073642',
    'bat CoV': 'MESH:D000073638',       # check
    'bovine CoV': 'MESH:D017938',
    'canine coronavirus': 'MESH:D017939',
    'canine CoV': 'MESH:D017939',
    'Chikungunya': 'MESH:D002646',
    'Ebolavirus': 'MESH:D029043',
    'Enterovirus 68': 'MESH:D030016',
    'equine CoV': 'MESH:D000073641',   # check
    'feline coronavirus': 'MESH:D016765',
    'feline CoV': 'MESH:D016765',
    'Human CoV 229E': 'MESH:D028941',
    'Human CoV HKU1': 'MESH:D000073640',
    'Human CoV NL63': 'MESH:D058957',
    'Human CoV OC43': 'MESH:D028962',
    'Lassa': 'MESH:D007836',
    'MERS-CoV': 'MESH:D065207',
    'murine coronavirus': 'MESH:D006517',  # check
    'murine CoV': 'MESH:D006517',  # check
    'Nipah': 'MESH:D045405',
    'porcine CoV': 'MESH:D045722',  # check
    'Rift Valley': 'MESH:D012296',  # check
    'SARS-CoV': 'MESH:D045473',
    'SARS-CoV-2': 'MESH:C000656484',
    'Zika virus': 'MESH:D000071244',
}

virus_label_map = {
    'MESH:C000656484': 'severe acute respiratory syndrome coronavirus 2'
}


def read_relations():
    df = pandas.read_csv(relations_fname, sep=',')
    df.fillna(value='')
    relations = {}
    for _, row in df.iterrows():
        relations[row['relation']] = {
            c: row[c] for c in df.columns[1:]
        }
    return relations


def read_evidences():
    df = pandas.read_csv(evidences_fname, sep=',')
    df.fillna(value='')
    evidences = defaultdict(list)
    for _, row in df.iterrows():
        evidences[row['relation']].append(
            {c: row[c] for c in df.columns[1:]}
        )
    return evidences


def get_drug_agent(name, id):
    matches = gilda.ground(name)
    if matches:
        db_refs = {matches[0].term.db: matches[0].term.id}
    else:
        if not id or ':' not in id:
            db_refs = {}
        else:
            db_ns, db_id = id.split(':', maxsplit=1)
            if db_ns == 'drugbank':
                db_refs = {'DRUGBANK': db_id}

    ag = Agent(name, db_refs=db_refs)
    standardize_agent_name(ag, standardize_refs=True)
    return ag


def get_virus_agent(name):
    db_ns, db_id = virus_grounding_map[name].split(':')
    db_refs = {db_ns: db_id}
    ag = Agent(name, db_refs=db_refs)
    mapped_label = virus_label_map.get('%s:%s' % (db_ns, db_id))
    if mapped_label:
        ag.name = mapped_label
    else:
        standardize_agent_name(ag, standardize_refs=True)
    return ag


def get_with_drug_statement(rel, evidences):
    drug = get_drug_agent(rel['result'], rel['resultID1'])
    virus = get_virus_agent(rel['virus'])
    stmt = Inhibition(drug, virus,
                      evidence=[get_evidence(ev)
                                for ev in evidences])
    return stmt


def get_evidence(ev):
    annot_args = ['pubIDRelationScore', 'sentenceRelationScore',
                  'researchStage']
    return Evidence(
        source_api='mitre_covid',
        text=ev.get('sentence'),
        pmid=ev.get('articleID'),
        annotations={arg: ev.get(arg) for arg in annot_args}
    )


if __name__ == '__main__':
    # There are 3 types of relations:
    # 'With Drug', 'Vaccine type', 'Entry Receptor'
    rels = read_relations()
    evs = read_evidences()
    with_drug_rels = {k: v for k, v in rels.items()
                      if v['relationType'] == 'With Drug'}
    with_drug_stmts = [
        get_with_drug_statement(rels[rel_id], evs[rel_id])
        for rel_id in with_drug_rels
    ]
