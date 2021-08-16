import os
import csv
import time
import json
import tqdm
import zlib
import logging
import argparse
import datetime
from copy import deepcopy
from itertools import groupby
from collections import defaultdict
from os.path import abspath, dirname, join
from indra.util import batch_iter
from indra_db.util import get_db
from indra_db.util import distill_stmts
from indra.statements import stmts_from_json, stmts_to_json
from indra.tools import assemble_corpus as ac
from indra.literature import pubmed_client
from covid_19.preprocess import get_ids, fix_doi, fix_pmid, get_metadata_dict, \
                                get_text_refs_from_metadata, download_metadata

logger = logging.getLogger(__name__)


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
    pmids = [fix_pmid(pmid) for pmid in get_ids('pubmed_id')]
    dois = [fix_doi(doi) for doi in get_ids('doi')]
    # Get unique text_refs from the DB
    db = get_db('primary')
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
    text_refs = [tr.get_ref_dict() for tr in trs]
    return text_refs


def get_text_refs_for_pubmed_search_term(search_term, **kwargs):
    """"Returns text ref IDs for PMIDs obtained using a PubMed search."""
    print('Searching for %s' % search_term)
    pmids = pubmed_client.get_ids(search_term, **kwargs)
    print('Getting TextRefs for %d PMIDs' % len(pmids))
    db = get_db('primary')
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


def get_reach_readings(tr_dicts, dump_dir=None, reach_version='1.6.1',
                       index_type='cord19', prioritize_by='type'):
    db = get_db('primary')
    # Get text ref dicts with article metadata aligned between DB and CORD19
    # Get REACH readings
    ts = time.time()
    logger.info('Querying for Reach outputs')
    reach_data = db.select_all((db.Reading, db.TextRef,
                                db.TextContent.source,
                                db.TextContent.text_type),
                               db.TextRef.id.in_(tr_dicts.keys()),
                               db.TextContent.text_ref_id == db.TextRef.id,
                               db.Reading.text_content_id == db.TextContent.id,
                               db.Reading.reader.like('REACH'),
                               db.Reading.reader_version.like(reach_version))
    te = time.time()
    logger.info('Finished querying for Reach outputs in %ss' % (te-ts))

    # Group readings by TextRef
    def tr_id_key_func(rd):
        return rd[1].id

    if prioritize_by == 'type':
        def content_priority_func(rd):
            text_type_priorities = {'fulltext': 0, 'abstract': 1, 'title': 2}
            source_priorities = {'pmc_oa': 0, 'manuscripts': 1, 'elsevier': 2,
                                 'cord19_pmc_xml': 3, 'cord19_pdf': 4,
                                 'cord19_abstract': 5, 'pubmed': 6}
            if rd[3] not in text_type_priorities:
                logger.info('Unhandled text type: %s' % rd[3])
            if rd[2] not in source_priorities:
                logger.info('Unhandled source: %s' % rd[2])
            return (rd[1].id,
                    text_type_priorities.get(rd[3], 100),
                    source_priorities.get(rd[2], 100))
    else:
        def content_priority_func(rd):
            return (rd[1].id,
                    -len(rd[0].bytes))

    # Sort by TextRef ID and content type/source
    logger.info('Sorting Reach outputs')
    reach_data.sort(key=content_priority_func)
    # Iterate over groups
    rds_filt = []
    logger.info('Prioritizing Reach outputs')
    for tr_id, tr_group in groupby(reach_data, tr_id_key_func):
        rds = list(tr_group)
        best_reading = rds[0]
        tr_dicts[tr_id]['READING_ID'] = best_reading.Reading.id
        rds_filt.append(best_reading)
    # If a dump directory is given, put all files in it
    trs_by_cord = {}
    logger.info('Dumping Reach outputs')
    if dump_dir:
        json_dir = join(dump_dir, 'json')
        if not os.path.exists(json_dir):
            os.mkdir(json_dir)
        for reading_result in tqdm.tqdm(rds_filt):
            reading = reading_result.Reading
            # If the reading output is empty, skip
            if not reading.bytes:
                continue
            tr = reading_result.TextRef
            text_ref = tr_dicts[tr.id]
            if index_type == 'cord19':
                cord_uid = text_ref['CORD19_UID']
                trs_by_cord[cord_uid] = text_ref
                fname = f'{cord_uid}.json'
            else:
                fname = f'{str(tr.id)}.json'
            with open(join(json_dir, fname), 'wt') as f:
                content = zlib.decompress(reading.bytes, 16+zlib.MAX_WBITS)
                f.write(content.decode('utf8'))
        if index_type == 'cord19':
            # Dump the metadata dictionary
            with open(join(dump_dir, 'metadata.json'), 'wt') as f:
                json.dump(trs_by_cord, f, indent=2)
    return rds_filt


def get_raw_stmts(tr_dicts, date_limit=None):
    """Return all raw stmts in INDRA DB for a given set of TextRef IDs.

    Parameters
    ----------
    tr_dicts : dict of text ref information
        Keys are text ref IDs (ints) mapped to dictionaries of text ref
        metadata.

    date_limit : Optional[int]
        A number of days to check the readings back.

    Returns
    -------
    list of stmts
        Raw INDRA Statements retrieved from the INDRA DB.
    """
    # Get raw statement IDs from the DB for the given TextRefs
    db = get_db('primary')
    # Get statements for the given text refs
    text_ref_ids = list(tr_dicts.keys())
    print(f"Distilling statements for {len(text_ref_ids)} TextRefs")
    start = time.time()
    clauses = [
        db.TextRef.id.in_(text_ref_ids),
        db.TextContent.text_ref_id == db.TextRef.id,
        db.Reading.text_content_id == db.TextContent.id,
        db.RawStatements.reading_id == db.Reading.id]
    if date_limit:
        start_date = (
            datetime.datetime.utcnow() - datetime.timedelta(days=date_limit))
        print(f'Limiting to stmts from readings in the last {date_limit} days')
        clauses.append(db.Reading.create_date > start_date)
    db_stmts = distill_stmts(db, get_full_stmts=True, clauses=clauses)
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


def cord19_metadata_for_trs(text_ref_dicts, md):
    """Get unified text_ref info given TextRef dictionaries and CORD19 metadata."""
    # Build up a sect of dictionaries for reverse lookup of TextRefs by
    # different IDs (DOI, PMC, PMID, etc.)
    tr_ids_by_doi = defaultdict(set)
    tr_ids_by_pmc = defaultdict(set)
    tr_ids_by_pmid = defaultdict(set)
    trs_by_trid = {}
    for tr_dict in text_ref_dicts:
        if tr_dict.get('DOI'):
            tr_ids_by_doi[tr_dict['DOI']].add(tr_dict['TRID'])
        if tr_dict.get('PMCID'):
            tr_ids_by_pmc[tr_dict['PMCID']].add(tr_dict['TRID'])
        if tr_dict.get('PMID'):
            tr_ids_by_pmid[tr_dict['PMID']].add(tr_dict['TRID'])
        trs_by_trid[tr_dict['TRID']] = tr_dict
    multiple_tr_ids = []
    mismatch_tr_ids = []
    tr_dicts = {}
    # Iterate over all the entries in the CORD19 metadata
    for md_row in md:
        tr_md = get_text_refs_from_metadata(md_row)
        # Find all the different TextRef IDs associated with the metadata
        # for this CORD19 araticle
        tr_ids_from_md = set()
        if 'DOI' in tr_md and tr_ids_by_doi.get(tr_md['DOI'].upper()):
            tr_ids_from_md |= tr_ids_by_doi[tr_md['DOI'].upper()]
        if 'PMCID' in tr_md and tr_ids_by_pmc.get(tr_md['PMCID']):
            tr_ids_from_md |= tr_ids_by_pmc[tr_md['PMCID']]
        if 'PMID' in tr_md and tr_ids_by_pmid.get(tr_md['PMID']):
            tr_ids_from_md |= tr_ids_by_pmid[tr_md['PMID']]
        # No TextRef for this CORD19 entry, so skip it
        if not tr_ids_from_md:
            continue
        # Multiple TextRef IDs for this CORD19 article
        if len(tr_ids_from_md) > 1:
            print("More than one TextRef:", tr_md, tr_ids_from_md)
            multiple_tr_ids.append(tr_ids_from_md)
        # Now,  TRID and update text ref dict
        for trid in tr_ids_from_md:
            tr_dict = trs_by_trid[trid]
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
            tr_dicts[tr_dict['TRID']] = tr_dict
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


def get_tr_dicts_and_ids():
    # Download metadata file if it is not in data directory
    download_metadata()
    # Get the text ref objects from the DB corresponding to the CORD19
    # articles
    text_refs = get_unique_text_refs()
    md = get_metadata_dict()
    tr_dicts, multiple_tr_ids = cord19_metadata_for_trs(text_refs, md)
    return tr_dicts, multiple_tr_ids


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
    tr_dicts, multiple_tr_ids = get_tr_dicts_and_ids()

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
            json.dump(tr_dicts, f, indent=2)
        multiple_trs = [('trid', 'pmid', 'pmcid', 'doi', 'manuscript_id')]
        for tr_set in multiple_tr_ids:
            for tr in tr_set:
                tr_data = (tr.id, tr.pmid, tr.pmcid, tr.doi, tr.manuscript_id)
                multiple_trs.append(tr_data)
        with open('multiple_tr_ids.csv', 'wt') as f:
            csvwriter = csv.writer(f, delimiter=',')
            csvwriter.writerows(multiple_trs)
