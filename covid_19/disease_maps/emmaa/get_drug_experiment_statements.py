"""This script gets curated drug screening experiments via hypothes.is
and brings them in a form where they can be applied on the COVID-19
Disease Map models."""
import copy
import pickle
from os.path import join, dirname, abspath
from indra.sources import reach, hypothesis
from emmaa.model_tests import StatementCheckingTest
from covid_19.get_hypothesis_stmts import filter_by_tag
from indra.ontology.standardize import standardize_name_db_refs


def filter_to_sars_cov_2(stmts):
    return list(filter(lambda x: x.obj.name == 'SARS-CoV-2', stmts))


def map_readout(stmts):
    for stmt in stmts:
        db_refs = {'TEXT': stmt.obj.db_refs['TEXT'],
                   'MESH': 'D014779'}
        stmt.obj.name, stmt.obj.db_refs = \
            standardize_name_db_refs(db_refs)
    return stmts


if __name__ == '__main__':
    # Note that REACH needs to be running locally for this to work
    reader = lambda txt: reach.process_text(txt, url=reach.local_text_url)
    # Access to the INDRA group needs to be available and configured for
    # this to work
    hp = hypothesis.process_annotations(reader=reader)
    print(f'{len(hp.statements)} statements from Hypothes.is.')
    # Filter to sources
    test_stmts = filter_by_tag(hp.statements, has={'test', 'covid19'})
    test_stmts = filter_to_sars_cov_2(test_stmts)
    test_stmts = map_readout(test_stmts)
    print(f'{len(test_stmts)} statements that will be used as tests.')

    scts = [StatementCheckingTest(stmt) for stmt in test_stmts]
    name = 'Literature-curated drug screening'
    description = 'A set of statements derived from literature-reported drug ' \
                  'screening hits for SARS-CoV-2.'
    tests = {'test': scts,
             'test_data': {'name': name, 'description': description}}
    stmts_file = join(dirname(abspath(__file__)), '..', '..', '..', 'stmts',
                      f'covid19_curated_tests_c19dm.pkl')
    with open(stmts_file, 'wb') as fh:
        pickle.dump(scts, fh)
