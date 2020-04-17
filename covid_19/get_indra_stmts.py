import os
import csv
import time
import json
import zlib
import argparse
from copy import deepcopy
from itertools import groupby
from collections import defaultdict
from os.path import abspath, dirname, join
from indra.util import batch_iter
from indra_db import get_primary_db
from indra_db.util import distill_stmts
from indra.statements import stmts_from_json, stmts_to_json
from indra.tools import assemble_corpus as ac
from indra.literature import pubmed_client
from covid_19.preprocess import get_ids, fix_doi, get_metadata_dict, \
                                get_text_refs_from_metadata


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
    dois = [fix_doi(doi) for doi in get_ids('doi')]
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
    trs = db.select_all(db.TextRef, db.TextRef.id.in_(ids))
    return trs


def get_text_refs_for_pubmed_search_term(search_term, **kwargs):
    """"Returns text ref IDs for PMIDs obtained using a PubMed search."""
    print('Searching for %s' % search_term)
    pmids = pubmed_client.get_ids(search_term, **kwargs)
    print('Getting TextRefs for %d PMIDs' % len(pmids))
    db = get_primary_db()
    tr_pmids = db.select_all(db.TextRef.id, db.TextRef.pmid_in(pmids))
    trids = {res.id for res in tr_pmids}
    return trids


def get_indradb_pa_stmts():
    """Get preassembled INDRA Stmts for PMC articles from INDRA DB.

    DEPRECATED. Get Raw Statements instead.
    """
    # Get the list of all PMCIDs from the corpus metadata
    pmcids = get_ids('pmcid')
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


def get_reach_readings(tr_dicts, dump_dir=None):
    db = get_primary_db()
    # Get text ref dicts with article metadata aligned between DB and CORD19
    # Get REACH readings 
    reach_data = db.select_all((db.Reading, db.TextRef,
                                db.TextContent.source,
                                db.TextContent.text_type),
                               db.TextRef.id.in_(tr_dicts.keys()),
                               db.TextContent.text_ref_id == db.TextRef.id,
                               db.Reading.text_content_id == db.TextContent.id,
                               db.Reading.reader == 'REACH')
    # Group readings by TextRef
    def tr_id_key_func(rd):
        return rd[1].id
    def content_priority_func(rd):
        text_type_priorities = {'fulltext': 0, 'abstract': 1, 'title': 2}
        source_priorities = {'pmc_oa': 0, 'manuscripts': 1, 'elsevier': 2,
                             'pubmed': 3}
        return (rd[1].id, text_type_priorities[rd[3]], source_priorities[rd[2]])
    # Sort by TextRef ID and content type/source
    reach_data.sort(key=content_priority_func)
    # Iterate over groups
    rds_filt = []
    for tr_id, tr_group in groupby(reach_data, tr_id_key_func):
        rds = list(tr_group)
        best_reading = rds[0]
        tr_dicts[tr_id]['READING_ID'] = best_reading.Reading.id
        rds_filt.append(best_reading)
    # If a dump directory is given, put all files in it
    trs_by_cord = {}
    if dump_dir:
        json_dir = join(dump_dir, 'json')
        os.mkdir(json_dir)
        for reading_result in rds_filt:
            tr = reading_result.TextRef
            reading = reading_result.Reading
            # If the reading output is empty, skip
            if not reading.bytes:
                continue
            text_ref = tr_dicts[tr.id]
            cord_uid = text_ref['CORD19_UID']
            trs_by_cord[cord_uid] = text_ref
            with open(join(json_dir, f'{cord_uid}.json'), 'wt') as f:
                content = zlib.decompress(reading.bytes, 16+zlib.MAX_WBITS)
                f.write(content.decode('utf8'))
        # Dump the metadata dictionary
        with open(join(dump_dir, 'metadata.json'), 'wt') as f:
            json.dump(trs_by_cord, f, indent=2)
    return rds_filt


def get_raw_stmts(tr_dicts):
    """Return all raw stmts in INDRA DB for a given set of TextRef IDs.

    Parameters
    ----------
    tr_dicts : dict of text ref information
        Keys are text ref IDs (ints) mapped to dictionaries of text ref
        metadata.

    Returns
    -------
    list of stmts
        Raw INDRA Statements retrieved from the INDRA DB.
    """
    # Get raw statement IDs from the DB for the given TextRefs
    db = get_primary_db()
    # Get statements for the given text refs
    text_ref_ids = list(tr_dicts.keys())
    print(f"Distilling statements for {len(text_ref_ids)} TextRefs")
    start = time.time()
    db_stmts = distill_stmts(db, get_full_stmts=True,
                             clauses=[
                                 db.TextRef.id.in_(text_ref_ids),
                                 db.TextContent.text_ref_id == db.TextRef.id,
                                 db.Reading.text_content_id == db.TextContent.id,
                                 db.RawStatements.reading_id == db.Reading.id])
    # Group lists of statements by the IDs TextRef that they come from
    stmts_by_trid = {}
    for stmt in db_stmts:
        trid = stmt.evidence[0].text_refs['TRID']
        if trid not in stmts_by_trid:
            stmts_by_trid[trid] = [stmt]
        else:
            stmts_by_trid[trid].append(stmt)
    # For every statement, update the text ref dictionary of the evidence
    # object with the aligned DB/CORD19 dictionaries obtained from the
    # function cord19_metadata_for_trs:
    stmts_flat = []
    for tr_id, stmt_list in stmts_by_trid.items():
        tr_dict = tr_dicts[tr_id]
        if tr_dict:
            for stmt in stmt_list:
                stmt.evidence[0].text_refs.update(tr_dict)
        stmts_flat += stmt_list
    elapsed = time.time() - start
    print(f"{elapsed} seconds")
    return stmts_flat


def dump_raw_stmts(tr_dicts, stmt_file):
    """Dump all raw stmts in INDRA DB for a given set of TextRef IDs.

    Parameters
    ----------
    tr_dicts : dict of text ref information
        Keys are text ref IDs (ints) mapped to dictionaries of text ref
        metadata.
    stmt_file : str
        Path to file to dump pickled raw statements.

    Returns
    -------
    list of stmts
        Raw INDRA Statements retrieved from the INDRA DB.
    """
    # Get the INDRA Statement JSON for the Statement IDs
    stmts_flat = get_raw_stmts(tr_dicts)
    ac.dump_statements(stmts_flat, stmt_file)
    return stmts_flat


def cord19_metadata_for_trs(text_refs, md, metadata_version='2020-04-24'):
    """Get unified text_ref info given TextRef objects and CORD19 metadata."""
    trs_by_doi = defaultdict(set)
    trs_by_pmc = defaultdict(set)
    trs_by_pmid = defaultdict(set)
    trs_by_trid = defaultdict(set)
    for tr in text_refs:
        if tr.doi:
            trs_by_doi[tr.doi].add(tr)
        if tr.pmcid:
            trs_by_pmc[tr.pmcid].add(tr)
        if tr.pmid:
            trs_by_pmid[tr.pmid].add(tr)
    multiple_tr_ids = []
    mismatch_tr_ids = []
    tr_dicts = {}
    for md_row in md:
        tr_md = get_text_refs_from_metadata(md_row,
                                            metadata_version=metadata_version)
        tr_ids_from_md = set()
        if 'DOI' in tr_md and trs_by_doi.get(tr_md['DOI'].upper()):
            tr_ids_from_md |= trs_by_doi[tr_md['DOI'].upper()]
        if 'PMCID' in tr_md and trs_by_pmc.get(tr_md['PMCID']):
            tr_ids_from_md |= trs_by_pmc[tr_md['PMCID']]
        if 'PMID' in tr_md and trs_by_pmid.get(tr_md['PMID']):
            tr_ids_from_md |= trs_by_pmid[tr_md['PMID']]
        if not tr_ids_from_md:
            continue
        if len(tr_ids_from_md) > 1:
            print("More than one TextRef:", tr_md, tr_ids_from_md)
            multiple_tr_ids.append(tr_ids_from_md)
        # No TextRef for this CORD19 entry, so skip it
        # Now, find all statements with this TRID and update text ref dict
        for tr in tr_ids_from_md:
            tr_dict = {'TRID': tr.id}
            if tr.pmcid:
                tr_dict['PMCID'] = tr.pmcid
            if tr.pmid:
                tr_dict['PMID'] = tr.pmid
            if tr.doi:
                tr_dict['DOI'] = tr.doi
            # Prefer IDs from the database wherever there is overlap
            for id_type in ('DOI', 'PMCID', 'PMID'):
                if id_type in tr_dict and id_type in tr_md:
                    if tr_md[id_type].upper() != tr_dict[id_type].upper():
                        print("Mismatch between INDRA DB and CORD19:",
                              tr_dict, tr_md)
                        mismatch_tr_ids.append((tr_dict, deepcopy(tr_md)))
                    tr_id_type = tr_md.pop(id_type)
            # Now that we've eliminated any overlaps, we can just update
            # the statement text ref dict
            tr_dict.update(tr_md)
            tr_dicts[tr.id] = tr_dict
    return tr_dicts, multiple_tr_ids


def combine_all_stmts(pkl_list, output_file):
    all_stmts = []
    for pkl_file in pkl_list:
        all_stmts.extend(ac.load_statements(pkl_file))
    ac.dump_statements(all_stmts, output_file)
    stmt_json = stmts_to_json(all_stmts)
    output_json = f"{output_file.rsplit('.', maxsplit=1)[0]}.json"
    with open(output_json, 'wt') as f:
        json.dump(stmt_json, f, indent=2)
    return all_stmts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Get INDRA DB content for CORD19 articles.')
    parser.add_argument('-m', '--mode',
                        help='Mode (stmts, reach, or tr_dicts)',
                        required=True)
    args = parser.parse_args()

    # Provide paths to all files
    stmts_dir = join(dirname(abspath(__file__)), '..', 'stmts')
    db_stmts_file = join(stmts_dir, 'cord19_all_db_raw_stmts.pkl')
    gordon_stmts_file = join(stmts_dir, 'gordon_ndex_stmts.pkl')
    eidos_stmts_file = join(stmts_dir, 'eidos_bio_statements_v2.pkl')
    combined_stmts_file = join(stmts_dir, 'cord19_combined_stmts.pkl')
    # Get the text ref objects from the DB corresponding to the CORD19
    # articles
    text_refs = get_unique_text_refs()
    md = get_metadata_dict()
    tr_dicts, multiple_tr_ids = cord19_metadata_for_trs(text_refs, md)

    if args.mode == 'stmts':
        db_stmts = dump_raw_stmts(tr_dicts, db_stmts_file)
        all_stmts = combine_all_stmts([db_stmts_file, gordon_stmts_file,
                                       eidos_stmts_file], combined_stmts_file)
    elif args.mode == 'reach':
        reach_readings = get_reach_readings(tr_dicts,
                                            dump_dir='cord19_reach_readings')
    elif args.mode == 'tr_dicts':
        # Dump tr_dicts as JSON file
        with open('tr_dicts.json', 'wt') as f:
            json.dump(tr_dicts, f, indent=2)a
        multiple_trs = [('trid', 'pmid', 'pmcid', 'doi', 'manuscript_id')]
        for tr_set in multiple_tr_ids:
            for tr in tr_set:
                tr_data = (tr.id, tr.pmid, tr.pmcid, tr.doi, tr.manuscript_id)
                multiple_trs.append(tr_data)
        with open('multiple_tr_ids.csv', 'wt') as f:
            csvwriter = csv.writer(f, delimiter=',')
            csvwriter.writerows(multiple_trs)
