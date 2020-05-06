import pickle
import argparse
from os.path import join, dirname, abspath
from indra.tools import assemble_corpus as ac

def stmts_by_text_refs(stmt_list):
    by_tr = {}
    no_tr = []
    for stmt in stmt_list:
        if len(stmt.evidence) > 1:
            raise ValueError('Statement has more than 1 evidence; '
                             'pass raw stmts')
        tr = stmt.evidence[0].text_refs.get('TRID')
        if tr is None:
            print(stmt)
            no_tr.append(stmt)
        else:
            if tr in by_tr:
                by_tr[tr].append(stmt)
            else:
                by_tr[tr] = [stmt]
    return by_tr, no_tr


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Put together updated EMMAA Statements for COVID-19 '
                        'model.')
    parser.add_argument('-om', '--old_mm',
                        help='Name of old Model Manager pkl file',
                        required=True)
    parser.add_argument('-oc', '--old_cord',
                        help='Name of old CORD-19 DB stmts pkl file',
                        required=True)
    parser.add_argument('-nc', '--new_cord',
                        help='Name of new CORD-19 DB stmts pkl file',
                        required=True)

    args = parser.parse_args()

    #stmts_dir = join(dirname(abspath(__file__)), '..', 'stmts')

    with open(args.old_mm, 'rb') as f:
        old_mm_emmaa_stmts = pickle.load(f)
        old_mm_stmts = [es.stmt for es in old_mm_emmaa_stmts]
    old_cord_stmts = ac.load_statements(args.old_cord)
    #new_cord_stmts = ac.load_statements(args.new_cord)


