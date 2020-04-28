import pickle
from emmaa.util import get_s3_client
from os.path import join, dirname, abspath
from indra.sources import reach, hypothesis
from indra.statements import RegulateActivity
from emmaa.model_tests import StatementCheckingTest


frieman_sars_cov_2_preprint = {'URL':
    'https://www.biorxiv.org/content/10.1101/2020.03.25.008482v2'}
frieman_sars_mers_paper = {'PMCID': 'PMC4136000'}

# These are the current standardized names for the viruses of interest
virus_names = {'severe acute respiratory syndrome coronavirus 2',
               'SARS Virus', 'Middle East Respiratory Syndrome Coronavirus'}


def has_text_ref(text_refs, refs):
    for ref in refs:
        for ref_key, ref_id in ref.items():
            if text_refs.get(ref_key) == ref_id:
                return True
    return False


def filter_text_refs(stmts, refs):
    return [s for s in stmts if has_text_ref(s.evidence[0].text_refs, refs)]


def separate_viral_regulation(stmts):
    viral_reg_stmts = []
    other_stmts = []
    for stmt in stmts:
        if isinstance(stmt, RegulateActivity) and stmt.obj.name in virus_names:
            viral_reg_stmts.append(stmt)
        else:
            other_stmts.append(stmts)
    return viral_reg_stmts, other_stmts


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


if __name__ == '__main__':
    # Note that REACH needs to be running locally for this to work
    reader = lambda txt: reach.process_text(txt, url=reach.local_text_url)
    # Access to the INDRA group needs to be available and configured for
    # this to work
    hp = hypothesis.process_annotations(reader=reader)
    print(f'{len(hp.statements)} statements from Hypothes.is.')
    # Filter to sources
    stmts = filter_text_refs(hp.statements, [frieman_sars_cov_2_preprint,
                                             frieman_sars_mers_paper])
    print(f'{len(stmts)} statements from sources of interest.')

    viral_reg_stmts, other_stmts = separate_viral_regulation(stmts)
    print(f'{len(viral_reg_stmts)} viral regulation statements that will be'
          f'used as tests and {len(other_stmts)} other statements.')

    updated_test_stmts = update_emmaa_tests(viral_reg_stmts)

    stmts_file = join(dirname(abspath(__file__)), '..', 'stmts',
                      'covid19_curated_tests.pkl')
    with open(stmts_file, 'wb') as fh:
        pickle.dump(updated_test_stmts, fh)

    stmts_file = join(dirname(abspath(__file__)), '..', 'stmts',
                      'hypothesis_stmts.pkl')
    with open(stmts_file, 'wb') as fh:
        pickle.dump(other_stmts, fh)
