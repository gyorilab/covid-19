import pickle
from emmaa.util import get_s3_client
from os.path import join, dirname, abspath
from indra.sources import reach, hypothesis
from indra.statements import RegulateActivity
from emmaa.model_tests import StatementCheckingTest


# These are the current standardized names for the viruses of interest
virus_names = {'severe acute respiratory syndrome coronavirus 2',
               'SARS Virus', 'Middle East Respiratory Syndrome Coronavirus'}


def update_emmaa_tests(viral_reg_stmts):
    client = get_s3_client()
    client.download_file(Bucket='emmaa',
                         Key='tests/covid19_curated_tests.pkl',
                         Filename='tmp.pkl')
    with open('tmp.pkl', 'rb') as fh:
        tests = pickle.load(fh)
    existing_hashes = {sct.stmt.get_hash() for sct in tests}
    for stmt in viral_reg_stmts:
        if stmt.get_hash() not in existing_hashes:
            tests.append(StatementCheckingTest(stmt))
    return tests


def filter_by_tag(stmts, has=None, not_has=None):
    has = has if has else set()
    not_has = not_has if not_has else set()
    stmts_out = []
    for stmt in stmts:
        tags = set(stmt.evidence[0].annotations['hypothes.is'].get('tags', []))
        if has <= tags and not (not_has & tags):
            stmts_out.append(stmt)
    return stmts_out


if __name__ == '__main__':
    # Note that REACH needs to be running locally for this to work
    reader = lambda txt: reach.process_text(txt, url=reach.local_text_url)
    # Access to the INDRA group needs to be available and configured for
    # this to work
    hp = hypothesis.process_annotations(reader=reader)
    print(f'{len(hp.statements)} statements from Hypothes.is.')
    # Filter to sources
    test_stmts = filter_by_tag(hp.statements, has={'test', 'covid19'})
    model_stmts = filter_by_tag(hp.statements, has={'indra', 'covid19'},
                                not_has={'test'})
    print(f'{len(test_stmts)} statements that will be'
          f'used as tests and {len(model_stmts)} other statements.')

    updated_test_stmts = update_emmaa_tests(test_stmts)

    stmts_file = join(dirname(abspath(__file__)), '..', 'stmts',
                      'covid19_curated_tests.pkl')
    with open(stmts_file, 'wb') as fh:
        pickle.dump(updated_test_stmts, fh)

    stmts_file = join(dirname(abspath(__file__)), '..', 'stmts',
                      'hypothesis_stmts.pkl')
    with open(stmts_file, 'wb') as fh:
        pickle.dump(model_stmts, fh)
