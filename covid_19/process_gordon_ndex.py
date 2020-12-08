from os.path import join, dirname, abspath
import pickle
from indra.sources import ndex_cx

mappings = {
    # Structural proteins
    'Spike': {'UP': 'P0DTC2'},
    'E': {'UP': 'P0DTC4'},
    'M': {'UP': 'P0DTC5'},
    'N': {'UP': 'P0DTC9'},
    # Proteins cleaved from Rp1a
    'Nsp1': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449635'},
    'Nsp2': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449636'},
    'Nsp3': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449637'},
    'Nsp4': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449638'},
    'Nsp5': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449639'},
    'Nsp5 C145A': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449639'},
    'Nsp6': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449640'},
    'Nsp7': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449641'},
    'Nsp8': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449642'},
    'Nsp9': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449643'},
    'Nsp10': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449644'},
    'Nsp11': {'UP': 'P0DTC1', 'UPPRO': 'PRO_0000449645'},
    # Proteins cleaved from Rp1ab
    'Nsp12': {'UP': 'P0DTD1', 'UPPRO': 'PRO_0000449629'},
    'Nsp13': {'UP': 'P0DTD1', 'UPPRO': 'PRO_0000449630'},
    'Nsp14': {'UP': 'P0DTD1', 'UPPRO': 'PRO_0000449630'},
    'Nsp15': {'UP': 'P0DTD1', 'UPPRO': 'PRO_0000449632'},
    'Nsp16': {'UP': 'P0DTD1', 'UPPRO': 'PRO_0000449633'},
    # Accessory factors
    'Orf3a': {'UP': 'P0DTC3'},
    'Orf3b': {},
    'Orf6': {'UP': 'P0DTC6'},
    'Orf7a': {'UP': 'P0DTC7'},
    'Orf7b': {'UP': 'P0DTD8'},
    'Orf8': {'UP': 'P0DTC8'},
    'Orf9b': {'UP': 'P0DTD2'},
    'Orf9c': {},
    'Orf10': {'UP': 'A0A663DJA2'},
    }


text_refs = {
    'DOI': '10.1038/s41586-020-2286-9',
    'PMID': '32353859',
    'PMCID': 'PMC7431030'
    }


def reground_stmts(stmts):
    for stmt in stmts:
        for agent in stmt.agent_list():
            if agent is not None and agent.name in mappings:
                refs = mappings[agent.name]
                agent.db_refs = refs
                agent.db_refs['TEXT'] = agent.name
        for ev in stmt.evidence:
            ev.pmid = text_refs['PMID']
            # Just to make a copy here
            ev.text_refs = {k: v for k, v in text_refs.items()}
    return stmts


if __name__ == '__main__':
    cxp = ndex_cx.process_ndex_network('43803262-6d69-11ea-bfdc-0ac135e8bacf',
                                       require_grounding=False)
    stmts = reground_stmts(cxp.statements)
    stmts_file = join(dirname(abspath(__file__)), '..', 'stmts',
                      'gordon_ndex_stmts.pkl')
    with open(stmts_file, 'wb') as fh:
        pickle.dump(stmts, fh)
