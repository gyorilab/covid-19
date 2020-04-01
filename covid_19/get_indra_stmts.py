import time
import json
from os.path import abspath, dirname, join
from indra.util import batch_iter
from indra_db import get_primary_db
from indra_db.util import distill_stmts
from indra.statements import stmts_from_json
from indra.tools import assemble_corpus as ac
from covid_19.preprocess import get_ids


def get_unique_text_refs():
    """Get unique INDRA DB TextRef IDs for all identifiers in CORD19.

    Queries TextRef IDs with PMIDs, PMCIDs, and DOIs from CORD19, then
    deduplicates to obtain a unique set of TextRefs.

    Returns
    -------
    set of ints
        Unique TextRef IDs.
    """
    pmcids = get_ids('pmcid')
    pmids = get_ids('pubmed_id')
    dois = get_ids('doi')
    # Get unique text_refs from the DB
    db = get_primary_db()
    print("Getting TextRefs by PMCID")
    tr_pmcids = db.select_all(db.TextRef.id, db.TextRef.pmcid_in(pmcids))
    print("Getting TextRefs by PMID")
    tr_pmids = db.select_all(db.TextRef.id, db.TextRef.pmid_in(pmids))
    tr_dois = []
    for ix, doi_batch in enumerate(batch_iter(dois, 10000)):
        print("Getting Text Refs by DOI batch", ix)
        tr_doi_batch = db.select_all(db.TextRef.id,
                            db.TextRef.doi_in(doi_batch, filter_ids=True))
        tr_dois.extend(tr_doi_batch)
    ids = set([res.id for res_list in (tr_dois, tr_pmcids, tr_pmids)
                      for res in res_list])
    print(len(ids), "unique TextRefs in DB")
    return ids


def get_indradb_pa_stmts():
    """Get preassembled INDRA Stmts for PMC articles from INDRA DB.

    DEPRECATED. Get Raw Statements instead.
    """
    # Get the list of all PMCIDs from the corpus metadata
    pmcids = get_pmcids()
    paper_refs = [('pmcid', p) for p in pmcids]
    stmt_jsons = []
    batch_size = 1000
    start = time.time()
    for batch_ix, paper_batch in enumerate(batch_iter(paper_refs, batch_size)):
        if batch_ix <= 5:
            continue
        papers = list(paper_batch)
        print("Querying DB for statements for %d papers" % batch_size)
        batch_start = time.time()
        result = get_statement_jsons_from_papers(papers)
        batch_elapsed = time.time() - batch_start
        batch_jsons = [stmt_json for stmt_hash, stmt_json
                                 in result['statements'].items()]
        print("Returned %d stmts in %f sec" %
              (len(batch_jsons), batch_elapsed))
        batch_stmts = stmts_from_json(batch_jsons)
        ac.dump_statements(batch_stmts, 'batch_%02d.pkl' % batch_ix)
        stmt_jsons += batch_jsons
    elapsed = time.time() - start
    print("Total time: %f sec, %d papers" % (elapsed, len(paper_refs)))
    stmts = stmts_from_json(stmt_jsons)
    ac.dump_statements(stmts, 'cord19_pmc_stmts.pkl')
    return stmt_jsons


def dump_indradb_raw_stmts(text_ref_ids, stmt_file):
    """Dump all raw stmts in INDRA DB for a given set of TextRef IDs.

    Parameters
    ----------
    text_ref_ids : list of ints
        TextRefs to get statements from.
    stmt_file : str
        Path to file to dump pickled raw statements.

    Returns
    -------
    list of stmts
        Raw INDRA Statements retrieved from the INDRA DB.
    """
    # Get raw statement IDs from the DB for the given TextRefs
    db = get_primary_db()
    print(f"Querying statement IDs for {len(text_ref_ids)} TextRefs")
    start = time.time()
    stmts = distill_stmts(db, get_full_stmts=True,
                          clauses=[
                            db.TextRef.id.in_(text_ref_ids),
                            db.TextContent.text_ref_id == db.TextRef.id,
                            db.Reading.text_content_id == db.TextContent.id,
                            db.RawStatements.reading_id == db.Reading.id])
    # Get the INDRA Statement JSON for the Statement IDs
    print(f"Getting JSON for {len(stmt_ids)} stmts")
    ac.dump_statements(stmts, stmt_file)
    elapsed = time.time() - start
    print(f"{elapsed} seconds")
    return stmts


def combine_all_stmts(pkl_list, output_file):
    all_stmts = []
    for pkl_file in pkl_list:
        all_stmts.extend(ac.load_statements(pkl_file))
    ac.dump_statements(all_stmts, output_file)
    return all_stmts

if __name__ == '__main__':
    # Provide paths to all files
    stmts_dir = join(dirname(abspath(__file__)), '..', 'stmts')
    db_stmts_file = join(stmts_dir, 'cord19_all_db_raw_stmts.pkl')
    gordon_stmts_file = join(stmts_dir, 'gordon_ndex_stmts.pkl')
    eidos_stmts_file = join(stmts_dir, 'eidos_bio_statements.pkl')
    combined_stmts_file = join(stmts_dir, 'cord19_combined_stmts.pkl')
    # Get all unique text refs in the DB with identifiers in the CORD19
    # corpus
    tr_ids = get_unique_text_refs()
    """
    # Get INDRA Statements from these text refs and dump to file
    db_stmts = dump_indradb_raw_stmts(list(tr_ids), db_stmts_file)
    # Combine with Eidos and Gordon et al. network statements
    stmts = combine_all_stmts([db_stmts_file, gordon_stmts_file,
                               eidos_stmts_file], combined_stmts_file)
    db = get_primary_db()
    text_refs = db.select_all(db.TextRef, db.TextRef.id.in_(tr_ids))
    """
