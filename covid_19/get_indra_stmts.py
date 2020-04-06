import os
import time
import json
import zlib
from copy import deepcopy
from itertools import groupby
from os.path import abspath, dirname, join
from indra.util import batch_iter
from indra_db import get_primary_db
from indra_db.util import distill_stmts
from indra.statements import stmts_from_json
from indra.tools import assemble_corpus as ac
from covid_19.preprocess import get_ids, fix_doi, load_metadata_dict, \
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


def get_reach_readings(tr_dicts, dump_dir=None):
    db = get_primary_db()
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
                print("Empty reading:")
                print("Reading ID:", reading.id)
                print("TextRef ID:", tr.id)
                print("Source:", reading_result.source)
                print("Text type:", reading_result.text_type)
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
    ac.dump_statements(list(stmts), stmt_file)
    elapsed = time.time() - start
    print(f"{elapsed} seconds")
    return stmts


def cord19_metadata_for_trs(text_refs, md, metadata_version='2020-03-27'):
    """Get unified text_ref info given TextRef objects and CORD19 metadata."""
    trs_by_doi = {}
    trs_by_pmc = {}
    trs_by_pmid = {}
    trs_by_trid = {}
    for tr in text_refs:
        if tr.doi:
            trs_by_doi[tr.doi] = tr
        if tr.pmcid:
            trs_by_pmc[tr.pmcid] = tr
        if tr.pmid:
            trs_by_pmid[tr.pmid] = tr
    multiple_tr_ids = []
    mismatch_tr_ids = []
    tr_dicts = {}
    for md_row in md:
        tr_md = get_text_refs_from_metadata(md_row,
                                            metadata_version=metadata_version)
        tr_ids_from_md = set()
        if 'DOI' in tr_md and trs_by_doi.get(tr_md['DOI'].upper()):
            tr_ids_from_md.add(trs_by_doi[tr_md['DOI'].upper()])
        if 'PMCID' in tr_md and trs_by_pmc.get(tr_md['PMCID']):
            tr_ids_from_md.add(trs_by_pmc[tr_md['PMCID']])
        if 'PMID' in tr_md and trs_by_pmid.get(tr_md['PMID']):
            tr_ids_from_md.add(trs_by_pmid[tr_md['PMID']])
        if len(tr_ids_from_md) > 1:
            print("More than one TextRef:", tr_md, tr_ids_from_md)
            tr = list(tr_ids_from_md)[0]
            multiple_tr_ids.append(tr_ids_from_md)
        elif len(tr_ids_from_md) == 1:
            tr_id = list(tr_ids_from_md)[0]
        # No TextRef for this CORD19 entry, so skip it
        else:
            continue
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
    return tr_dicts


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
    text_refs = get_unique_text_refs()
    md = load_metadata_dict()
    tr_dicts = cord19_metadata_for_trs(text_refs, md,
                                       metadata_version='2020-04-03')
    reach_readings = get_reach_readings(tr_dicts,
                                        dump_dir='cord19_reach_readings')

    # Get INDRA Statements from these text refs and dump to file
    #db_stmts = dump_indradb_raw_stmts(list(tr_ids), db_stmts_file)
    db_stmts = list(ac.load_statements(db_stmts_file))

    # Dict of statements by TextRef ID
    stmts_by_trid = {}
    for stmt in db_stmts:
        trid = stmt.evidence[0].text_refs['TRID']
        if trid not in stmts_by_trid:
            stmts_by_trid[trid] = [stmt]
        else:
            stmts_by_trid[trid].append(stmt)
    """
    db = get_primary_db()
    stmt_textrefs = db.select_all(db.TextRef,
                                  db.TextRef.id.in_(stmts_by_trid.keys()))

    trs_by_doi = {}
    trs_by_pmc = {}
    trs_by_pmid = {}
    for tr in stmt_textrefs:
        if tr.doi:
            trs_by_doi[tr.doi] = tr.id
        if tr.pmcid:
            trs_by_pmc[tr.pmcid] = tr.id
        if tr.pmid:
            trs_by_pmid[tr.pmid] = tr.id
    md = read_metadata('data/2020-03-27/metadata.csv')
    multiple_tr_ids = []
    mismatch_tr_ids = []
    for md_row in md:
        tr_md = get_text_refs_from_metadata(md_row,
                                            metadata_version='2020-03-27')
        tr_ids_from_md = set()
        if 'DOI' in tr_md and trs_by_doi.get(tr_md['DOI'].upper()):
            tr_ids_from_md.add(trs_by_doi[tr_md['DOI'].upper()])
        if 'PMCID' in tr_md and trs_by_pmc.get(tr_md['PMCID']):
            tr_ids_from_md.add(trs_by_pmc[tr_md['PMCID']])
        if 'PMID' in tr_md and trs_by_pmid.get(tr_md['PMID']):
            tr_ids_from_md.add(trs_by_pmid[tr_md['PMID']])
        if len(tr_ids_from_md) > 1:
            print("More than one TRID:", tr_md, tr_ids_from_md)
            tr_id = list(tr_ids_from_md)[0]
            multiple_tr_ids.append(tr_ids_from_md)
        elif len(tr_ids_from_md) == 1:
            tr_id = list(tr_ids_from_md)[0]
        # No match, so skip this CORD19 entry
        else:
            continue
        # Now, find all statements with this TRID and update text ref dict
        for tr_id in tr_ids_from_md:
            tr_stmts = stmts_by_trid[tr_id]
            for stmt in tr_stmts:
                stmt_tr = stmt.evidence[0].text_refs
                # Prefer IDs from the database wherever there is overlap
                for id_type in ('DOI', 'PMCID', 'PMID'):
                    if id_type in stmt_tr and id_type in tr_md:
                        if tr_md[id_type].upper() != stmt_tr[id_type].upper():
                            print("Mismatch between INDRA DB and CORD19:",
                                  stmt_tr, tr_md)
                            mismatch_tr_ids.append((stmt_tr, deepcopy(tr_md)))
                        tr_id_type = tr_md.pop(id_type)
                # Now that we've eliminated any overlaps, we can just update
                # the statement text ref dict
                stmt_tr.update(tr_md)

    # Combine with Eidos and Gordon et al. network statements
    stmts = combine_all_stmts([db_stmts_file, gordon_stmts_file,
                               eidos_stmts_file], combined_stmts_file)
    db = get_primary_db()
    text_refs = db.select_all(db.TextRef, db.TextRef.id.in_(tr_ids))
    """

