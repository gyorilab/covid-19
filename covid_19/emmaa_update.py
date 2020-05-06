import pickle
import argparse
from copy import copy
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
            no_tr.append(stmt)
        else:
            if tr in by_tr:
                by_tr[tr].append(stmt)
            else:
                by_tr[tr] = [stmt]
    return by_tr, no_tr


def combine_stmts(new_cord_by_tr, old_mm_by_tr):
    stmts_copy = copy(new_cord_by_tr)
    for trid, stmts in old_mm_by_tr.items():
        if trid not in stmts_copy:
            stmts_copy[trid] = stmts
    return stmts_copy


if __name__ == '__main__':
    # Example:
    # run covid_19/emmaa_update.py
    #            -om stmts/model_2020-04-02-17-01-44.pkl
    #            -nc stmts/cord19_all_db_raw_stmts.pkl
    #            -d stmts/drug_stmts.pkl
    #            -g stmts/gordon_ndex_stmts.pkl
    #            -f stmts/cord19_combined_stmts.pkl
    parser = argparse.ArgumentParser(
            description='Put together updated EMMAA Statements for COVID-19 '
                        'model.')
    parser.add_argument('-om', '--old_mm',
                        help='Name of old Model Manager pkl file',
                        required=True)
    parser.add_argument('-nc', '--new_cord',
                        help='Name of new CORD-19 DB stmts pkl file',
                        required=True)
    parser.add_argument('-d', '--drug_stmts',
                         help='Path to drug statements pkl file',
                         required=True)
    parser.add_argument('-g', '--gordon_stmts',
                         help='Path to Gordon statements pkl file',
                         required=True)
    parser.add_argument('-f', '--output_file',
                         help='Output file for combined pkl',
                         required=True)
    args = parser.parse_args()

    # Load everything
    with open(args.old_mm, 'rb') as f:
        old_mm_emmaa_stmts = pickle.load(f)
        old_mm_stmts = [es.stmt for es in old_mm_emmaa_stmts]
    new_cord_stmts = ac.load_statements(args.new_cord)
    drug_stmts = ac.load_statements(args.drug_stmts)
    gordon_stmts = ac.load_statements(args.gordon_stmts)

    # Filter out ungrounded statements
    new_cord_grounded = ac.filter_grounded_only(new_cord_stmts)

    # Group statements by TextRef
    old_mm_by_tr, old_mm_no_tr = stmts_by_text_refs(old_mm_stmts)
    new_cord_by_tr, new_cord_no_tr = stmts_by_text_refs(new_cord_grounded)

    # Add any ModelManager statements from non-Cord19 publications
    updated_mm_stmts_by_tr = combine_stmts(new_cord_by_tr, old_mm_by_tr)
    updated_mm_stmts = [s for stmt_list in updated_mm_stmts_by_tr.values()
                          for s in stmt_list]

    # Now, add back in the drug stmts and Gordon PPI stmts
    combined_stmts = updated_mm_stmts + drug_stmts + gordon_stmts

    # Dump new pickle
    ac.dump_statements(combined_stmts, args.output_file)


