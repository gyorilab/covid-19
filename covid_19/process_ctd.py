import logging
import pickle
import argparse
import os
from collections import defaultdict
from indra.tools import assemble_corpus as ac
from emmaa.model import get_assembled_statements
from emmaa.model_tests import load_tests_from_s3


logger = logging.getLogger(__name__)


def get_groundings(stmts, ns, cutoff=None):
    if cutoff is None:
        cutoff = 1
    grounding_counts = defaultdict(int)
    for stmt in stmts:
        for ag in stmt.agent_list():
            if ag is not None:
                gr = ag.get_grounding()
                if ns in gr:
                    grounding_counts[gr] += len(stmt.evidence)
    logger.info('Found %d unique groundings with %s namespace.'
                % (len(grounding_counts), ns))
    groundings = [
        gr for gr in grounding_counts if grounding_counts[gr] >= cutoff]
    logger.info('%d groundings with at least %d mentions.'
                % (len(groundings), cutoff))
    return groundings


def filter_by_groundings(stmts, groundings, policy='any'):
    logger.info('Filtering %d statements to those that have %s agents '
                'grounded to one of %d groundings.'
                % (len(stmts), policy, len(groundings)))
    stmts_out = []
    for stmt in stmts:
        ag_groundings = [
            ag.get_grounding() for ag in stmt.agent_list() if ag is not None]
        if policy == 'any':
            if any([ag_gr in groundings for ag_gr in ag_groundings]):
                stmts_out.append(stmt)
        elif policy == 'all':
            if all([ag_gr in groundings for ag_gr in ag_groundings]):
                stmts_out.append(stmt)
    logger.info('%d statements after filter' % len(stmts_out))
    return stmts_out


if __name__ == '__main__':
    # Example:
    # python covid_19/process_ctd.py \
    #       -cd stmts/ctd_chemical_disease.pkl
    #       -cg stmts/ctd_chemical_gene.pkl
    #       -gd stmts/ctd_gene_disease.pkl
    parser = argparse.ArgumentParser(
        description='Filter CTD statements to include in EMMAA COVID-19 model')
    parser.add_argument('-cd', '--chemical_disease',
                        help='Path to ctd chemical disease statements pkl',
                        required=True)
    parser.add_argument('-cg', '--chemical_gene',
                        help='Path to ctd chemical gene statements pkl',
                        required=True)
    parser.add_argument('-gd', '--gene_disease',
                        help='Path to ctd gene disease statements pkl',
                        required=True)
    args = parser.parse_args()

    # Load model statements and tests
    model_stmts = get_assembled_statements('covid19')
    curated_tests, _ = load_tests_from_s3('covid19_curated_tests')
    if isinstance(curated_tests, dict):  # if descriptions were added
        curated_tests = curated_tests['tests']
    mitre_tests, _ = load_tests_from_s3('covid19_mitre_tests')
    if isinstance(mitre_tests, dict):  # if descriptions were added
        mitre_tests = mitre_tests['tests']
    all_test_stmts = [test.stmt for test in curated_tests] + \
        [test.stmt for test in mitre_tests]

    # Load CTD statements
    chem_dis_stmts = ac.load_statements(args.chemical_disease)
    chem_gene_stmts = ac.load_statements(args.chemical_gene)
    gene_dis_stmts = ac.load_statements(args.gene_disease)
    all_ctd_stmts = chem_dis_stmts + chem_gene_stmts + gene_dis_stmts

    # Collect most frequents gene groundings for model statements and
    # chemical groundings for test statements
    model_gene_groundings = get_groundings(model_stmts, 'HGNC', cutoff=100)
    chem_test_groundings = get_groundings(all_test_stmts, 'CHEBI', None)
    all_groundings = model_gene_groundings + chem_test_groundings
    # Filter ctd statements to those having agents grounded to found groundings
    filtered_stmts = filter_by_groundings(all_ctd_stmts, all_groundings)
    fname = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         '..', 'stmts', 'ctd_stmts.pkl')
    with open(fname, 'wb') as fh:
        pickle.dump(filtered_stmts, fh)
