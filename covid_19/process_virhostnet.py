import logging
import os
import pickle
from indra.sources.virhostnet import process_from_web


logger = logging.getLogger(__name__)


virus_taxonomy_ids = {
    'avian CoV': '694014',
    'bat CoV': '1508220',
    'bovine CoV': '11128',
    'canine coronavirus': '11153',
    'canine CoV': '11153',
    'Chikungunya': '37124',
    'Ebolavirus': '186536',
    'Enterovirus 68': '42789',
    'equine CoV': '136187',
    'feline coronavirus': '12663',
    'feline CoV': '12663',
    'Human CoV 229E': '11137',
    'Human CoV HKU1': '290028',
    'Human CoV NL63': '277944',
    'Human CoV OC43': '31631',
    'Lassa': '11620',
    'MERS-CoV': '1335626',
    'murine coronavirus': '694005',
    'murine CoV': '694005',
    'Nipah': '121791',
    'porcine CoV': '694013',
    'Rift Valley': '11588',
    'SARS-CoV': '694009',
    'SARS-CoV-2': '2697049',
    'Zika virus': '64320',
}


def filter_to_tax_ids(stmts, virus_tax_ids, gene_tax_ids):
    logger.info('Filtering %d statements for taxonomy IDs' % len(stmts))
    stmts_out = []
    for stmt in stmts:
        if stmt.evidence[0].annotations['vir_tax'] in virus_tax_ids and \
                stmt.evidence[0].annotations['host_tax'] in gene_tax_ids:
            stmts_out.append(stmt)
    logger.info('%d statements after filter' % len(stmts_out))
    return stmts_out


if __name__ == '__main__':
    # Get all INDRA statements from VirHostNet
    vp = process_from_web()
    # Filter to only included viruses in virus_taxonomy_ids map and human genes
    stmts = filter_to_tax_ids(
        vp.statements, virus_taxonomy_ids.values(), ['9606'])
    fname = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         '..', 'stmts', 'virhostnet_stmts.pkl')
    with open(fname, 'wb') as fh:
        pickle.dump(stmts, fh)
