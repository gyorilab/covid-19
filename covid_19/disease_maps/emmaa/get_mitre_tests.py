import pickle
from indra.statements import Agent
from os.path import join, dirname, abspath, pardir
stmts_path = join(dirname(abspath(__file__)), pardir, pardir, 'stmts')


if __name__ == '__main__':
    with open(join(stmts_path, 'covid19_mitre_tests_2021-02-10-22-51-43.pkl'),
              'rb') as fh:
        mitre_tests = pickle.load(fh)

    test_stmts = mitre_tests['tests']
    test_stmts = [s for s in test_stmts if s.stmt.obj.name == 'SARS-CoV-2']
    for stmt in test_stmts:
        stmt.stmt.obj = Agent('COVID-19 adverse outcomes')
    mitre_tests['tests'] = test_stmts

    with open(join(stmts_path, 'covid19_mitre_tests_c19dm.pkl'), 'wb') as fh:
        pickle.dump(mitre_tests, fh)
