import pickle
import argparse
import logging
from copy import copy
from indra.tools import assemble_corpus as ac
from covid_19.get_indra_stmts import get_tr_dicts_and_ids, get_raw_stmts


logger = logging.getLogger(__name__)


def stmts_by_text_refs(stmt_list):
    by_tr = {}
    no_tr = []
    for stmt in stmt_list:
        #if len(stmt.evidence) > 1:
        #    raise ValueError('Statement has more than 1 evidence; '
        #                     'pass raw stmts')
        tr = stmt.evidence[0].text_refs.get('TRID')
        if tr is None:
            no_tr.append(stmt)
        else:
            if tr in by_tr:
                by_tr[tr].append(stmt)
            else:
                by_tr[tr] = [stmt]
    return by_tr, no_tr


def combine_stmts(new_cord_by_tr, old_model_by_tr):
    stmts_copy = copy(new_cord_by_tr)
    for trid, stmts in old_model_by_tr.items():
        if trid not in stmts_copy:
            stmts_copy[trid] = stmts
    return stmts_copy


def make_model_stmts(old_model_stmts, new_cord_stmts=None, date_limit=5):
    """Process and combine statements from different resources.

    Parameters
    ----------
    old_model_stmts : list[indra.statements.Statement]
        A list of statements currently in the model.
    new_cord_stmts : Optional[list[indra.statements.Statement]]
        A list of newly extracted statements from CORD19 corpus. If not
        provided, the statements are pulled from the database and filtered
        to those not in old_model_stmts.
    date_limit : Optional[int]
        How many days back to search the database for CORD19 statements.
        Default: 5.

    Returns
    -------
    updated_model_stmts : list[indra.statements.Statement]
        A list of statements to make a new model from.
    paper_ids : list[str]
        A list of TRIDs associated with statements.
    """
    # If new cord statements are not provided, load from database
    if not new_cord_stmts:
        # Get text refs from metadata
        tr_dicts, _ = get_tr_dicts_and_ids()
        # Filter to text refs that are not part of old model
        new_tr_dicts = {}
        old_tr_ids = set()
        for stmt in old_model_stmts: 
            for evid in stmt.evidence: 
                if evid.text_refs.get('TRID'): 
                    old_tr_ids.add(evid.text_refs['TRID'])
        for tr_id in tr_dicts: 
            if tr_id not in old_tr_ids: 
                new_tr_dicts[tr_id] = tr_dicts[tr_id]
        logger.info('Found %d TextRefs, %d of which are not in old model'
                    % (len(tr_dicts), len(new_tr_dicts)))
        # Get statements for new text re
        new_cord_stmts = get_raw_stmts(new_tr_dicts, date_limit=date_limit)

    logger.info('Processing the statements')
    # Filter out ungrounded statements
    new_cord_grounded = ac.filter_grounded_only(new_cord_stmts)

    # Group statements by TextRef
    old_model_by_tr, _ = stmts_by_text_refs(old_model_stmts)
    new_cord_by_tr, _ = stmts_by_text_refs(new_cord_grounded)

    # Add any EMMAA statements from non-Cord19 publications
    updated_model_stmts_by_tr = combine_stmts(new_cord_by_tr, old_model_by_tr)
    updated_model_stmts = [
        s for stmt_list in updated_model_stmts_by_tr.values()
        for s in stmt_list]

    logger.info('Got %d total statements.' % len(updated_model_stmts))
    logger.info('Processed %d papers.' % len(updated_model_stmts_by_tr))
    return updated_model_stmts, updated_model_stmts_by_tr.keys()

if __name__ == '__main__':
    # Example:
    # python covid_19/emmaa_update.py \
    #            -om stmts/model_2020-05-17-17-10-07.pkl \
    #            -nc stmts/cord19_all_db_raw_stmts.pkl \
    #            -d stmts/drug_stmts.pkl \
    #            -g stmts/gordon_ndex_stmts.pkl \
    #            -v stmts/virhostnet_stmts.pkl \
    #            -c stmts/ctd_stmts.pkl
    #            -f stmts/cord19_combined_stmts.pkl
    parser = argparse.ArgumentParser(
            description='Put together updated statement pkl for COVID-19 '
                        'EMMAA model.')
    parser.add_argument('-om', '--old_model',
                        help='Name of old EMMAA model pkl file',
                        required=True)
    parser.add_argument('-nc', '--new_cord',
                        help='Name of new CORD-19 DB stmts pkl file (optional)',
                        required=False)
    parser.add_argument('-d', '--drug_stmts',
                         help='Path to drug statements pkl file',
                         required=True)
    parser.add_argument('-g', '--gordon_stmts',
                         help='Path to Gordon statements pkl file',
                         required=True)
    parser.add_argument('-v', '--virhostnet_stmts',
                        help='Path to VirHostNet statements pkl file',
                        required=True)
    parser.add_argument('-c', '--ctd_stmts',
                        help='Path to CTD statements pkl file',
                        required=True)
    parser.add_argument('-f', '--output_file',
                         help='Output file for combined pkl',
                         required=True)
    args = parser.parse_args()

    # Load everything
    logger.info('Loading statements from pickle files')
    with open(args.old_model, 'rb') as f:
        old_model_emmaa_stmts = pickle.load(f)
        old_model_stmts = [es.stmt for es in old_model_emmaa_stmts]
    if args.new_cord:
        new_cord_stmts = ac.load_statements(args.new_cord)
    else:
        new_cord_stmts = None
    drug_stmts = ac.load_statements(args.drug_stmts)
    gordon_stmts = ac.load_statements(args.gordon_stmts)
    virhostnet_stmts = ac.load_statements(args.virhostnet_stmts)
    ctd_stmts = ac.load_statements(args.ctd_stmts)

    other_stmts = drug_stmts + gordon_stmts + virhostnet_stmts + ctd_stmts

    combined_stmts, _ = make_model_stmts(
        old_model_stmts, new_cord_stmts)

    # Dump new pickle
    ac.dump_statements(combined_stmts, args.output_file)
