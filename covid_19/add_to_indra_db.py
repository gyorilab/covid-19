import logging
import pandas as pd
from indra.util import batch_iter
from indra_db import get_primary_db
from indra.literature import id_lookup
from indra.literature import pubmed_client
from indra_db.managers import content_manager
from indra_db.util.data_gatherer import DataGatherer, DGContext
from indra_db.managers.content_manager import PmcManager, ContentManager
from covid_19.get_indra_stmts import get_unique_text_refs, get_metadata_dict, \
                                     cord19_metadata_for_trs
from covid_19.preprocess import get_text_refs_from_metadata, \
                                get_texts_for_entry

logger = logging.getLogger(__name__)


content_manager.logger.setLevel(logging.DEBUG)


gatherer = DataGatherer('content', ['refs', 'content'])


class Cord19Manager(ContentManager):
    my_source = 'cord19'
    #tr_cols = ('pmid', 'pmcid', 'doi', 'cord19_uid')
    tr_cols = ('pmid', 'pmcid', 'doi')

    def __init__(self, cord_md):
        self.cord_md = cord_md
        self.tr_data = []
        self.tc_data = []
        self.review_fname = 'cord19_mgr_review.txt'

        import random
        random.shuffle(cord_md)
        # Get tr_data list from Cord19 metadata
        # tr_data is a list [{'pmid': xxx, 'pmcid': xxx}, {...}]
        # tc_data is a list of dictionaries keyed by column name (???)
        for ix, md_entry in enumerate(cord_md[0:1000]):
            if ix % 10000 == 0:
                print(f"Processing CORD-19 full texts: {ix} of {len(cord_md)}")
            text_refs = get_text_refs_from_metadata(md_entry)
            doi = text_refs.get('DOI')
            if doi is not None:
                doi = doi.upper()
            tr_data_entry = {'pmid': text_refs.get('PMID'),
                             'pmcid': text_refs.get('PMCID'),
                             'doi': doi,
                             'cord_uid': text_refs.get('CORD_UID')}
            source_type = md_entry['full_text_file']
            # If has abstract, add TC entry with abstract content
            tc_texts = get_texts_for_entry(md_entry)
            for text_type, text in tc_texts:
                tc_data_entry = {'text': text,
                                 'source': text_type,
                                 'format': 'text'}
                tc_data_entry.update(tr_data_entry)
                self.tc_data.append(tc_data_entry)
            self.tr_data.append(tr_data_entry)


    def filter_text_content(self, db, tc_data):
        """Filter the text content to identify pre-existing records."""
        if not len(tc_data):
            return []
        logger.info("Beginning to filter text content...")
        tr_list = []
        for ix, tc_batch in enumerate(batch_iter(tc_data, 10000)):
            # Get the sets of IDs for this batch
            pmid_set = set([tc['pmid'] for tc in tc_data if tc['pmid']])
            pmcid_set = set([tc['pmcid'] for tc in tc_data if tc['pmcid']])
            doi_set = set([tc['doi'] for tc in tc_data if tc['doi']])

            # Step 1: Build a dictionary matching IDs to text ref objects
            logger.debug("Getting text refs for pmcid->trid dict..")
            # Get all TextRefs for the
            tr_list = db.select_all(db.TextRef, sql_exp.or_(
                            db.TextRef.pmid_in(pmid_set, filter_ids=True),
                            db.TextRef.pmcid_in(pmcid_set, filter_ids=True),
                            db.TextRef.doi_in(doi_set, filter_ids=True)))
        # Next, build dictionaries mapping IDs back to TextRef objects so
        # that we can link records in tc_data to TextRefs
        trs_by_doi = defaultdict(set)
        trs_by_pmc = defaultdict(set)
        trs_by_pmid = defaultdict(set)
        for tr in text_refs:
            if tr.doi:
                trs_by_doi[tr.doi].add(tr)
            if tr.pmcid:
                trs_by_pmc[tr.pmcid].add(tr)
            if tr.pmid:
                trs_by_pmid[tr.pmid].add(tr)

        pmcid_trid_dict = {
            pmcid: trid for (pmcid, trid) in
                            db.get_values(tref_list, ['pmcid', 'id'])
            }
        # Step 2: Get existing Text Content objects corresponding to the
        # the given text refs with the same format and source
        # This should be a very small list, in general.
        logger.debug('Finding existing text content from db.')
        existing_tcs = db.select_all(
            db.TextContent,
            db.TextContent.text_ref_id.in_([tr.id for tr in tr_list]),
            db.TextContent.source == self.,
            db.TextContent.format == formats.XML
            )

        # Reformat Text Content objects to list of tuples
        existing_tc_records = [
            (tc.text_ref_id, tc.source, tc.format, tc.text_type)
            for tc in existing_tcs
            ]
        logger.debug("Found %d existing records on the db."
                     % len(existing_tc_records))
        tc_records = []
        # Now, iterate over the TC data dictionary and build up tc_records,
        # which is the list of tuples that a will be inserted into the DB
        for tc in tc_data:
            if tc['pmcid'] not in pmcid_trid_dict.keys():
                # In principle this shouldn't happen, because the set of
                # TCs in tc_data corresponds to the set of TRs that are
                # either in the DB already or were just newly inserted,
                # and any records with ID issues should have been filtered
                # out
                logger.warning("Found pmcid (%s) among text content data, but "
                               "not in the database. Skipping." % tc['pmcid'])
                continue
            tc_records.append(
                (
                    # First entry is TR object, not TR ID
                    pmcid_trid_dict[tc['pmcid']],
                    self.my_source,
                    formats.XML,
                    tc['text_type'],
                    tc['content']
                    )
                )
        # Filter the TC records to exclude 
        filtered_tc_records = [
            rec for rec in tc_records if rec[:-1] not in existing_tc_records
            ]
        logger.info("Finished filtering the text content.")
        return list(set(filtered_tc_records))



    def populate(self, db):
        # Turn the list of dicts into a set of tuples
        tr_data_set = {tuple([entry[id_type] for id_type in self.tr_cols])
                       for entry in self.tr_data}
        # FIXME HACK: Manually remove the broken DOI
        #tr_data_set = set([t for t in tr_data_set
        #                     if t[2] != '0.1126/SCIENCE.ABB7331'])
        # Filter_text_refs will figure out which articles are already in the
        # TextRef table and will update them with any new metadata;
        # filtered_tr_records are the ones that need to be added to the DB
        filtered_tr_records = []
        flawed_tr_records = []
        for ix, tr_batch in enumerate(batch_iter(tr_data_set, 10000)):
            print("Getting Text Refs using pmid/pmcid/doi, batch", ix)
            filt_batch, flaw_batch = \
                    self.filter_text_refs(db, set(tr_batch),
                                    primary_id_types=['pmid', 'pmcid', 'doi'])
            filtered_tr_records.extend(filt_batch)
            flawed_tr_records.extend(flaw_batch)

        trs_to_skip = {rec for cause, rec in flawed_tr_records}

        # Why did the original version not skip in case of disagreeing
        # pmid or doi?
        #pmcids_to_skip = {rec[self.tr_cols.index('pmcid')]
        #                  for cause, rec in flawed_tr_records
        #                  if cause in ['pmcid', 'over_match_input',
        #                               'over_match_db']}

        # Then we put together the updated text content data
        if len(trs_to_skip) is not 0:
            mod_tc_data = [
                tc for tc in self.tc_data
                if (tc.get('pmid'), tc.get('pmcid'), tc.get('doi'))
                                                    not in trs_to_skip]
        else:
            mod_tc_data = tc_data

        # Upload TextRef data for articles NOT already in the DB
        logger.info('Adding %d new text refs...' % len(filtered_tr_records))
        #self.copy_into_db(
        #    db,
        #    'text_ref',
        #    filtered_tr_records,
        #    self.tr_cols
        #    )
        # FIXME gatherer.add('refs', len(filtered_tr_records))

        import ipdb; ipdb.set_trace()

        # Process the text content data
        filtered_tc_records = self.filter_text_content(db, mod_tc_data)


        # Upload the text content data.
        logger.info('Adding %d more text content entries...' %
                    len(filtered_tc_records))
        #self.copy_into_db(
        #    db,
        #    'text_content',
        #    filtered_tc_records,
        #    self.tc_cols
        #    )
        gatherer.add('content', len(filtered_tc_records))
        return



if __name__ == '__main__':
    md = get_metadata_dict()
    cm = Cord19Manager(md)
    db = get_primary_db()
    cm.populate(db)


    #text_refs = get_unique_text_refs()
    #tr_dicts = cord19_metadata_for_trs(text_refs, md)

    # All entries in tr_dicts have

    # Columns
    """
    df = get_file_data()
    tr_data = []
    for row in df.itertuples():
        pmid = get_val(row, 'pubmed_id')
        pmcid = get_val(row, 'pmcid')
        doi = get_val(row, 'doi')
        # Manuscript ID is None
        tr_entry = {'pmid': pmid, 'pmcid': pmcid, 'doi': doi,
                    'manuscript_id': None}
        tr_data.append(tr_entry)
    db = get_primary_db()
    cm = Cord19Manager()
    missing_pmids = cm.upload_batch(db, tr_data, None)
    """
    """
    import pickle
    with open('../tr_data_with_pmids.pkl', 'rb') as f:
        tr_data = pickle.load(f)
    tr_data_doi = [tr for tr in tr_data
                   if tr['pmid'] is None and tr['pmcid'] is None and
                      tr['doi'] is not None]
    import random
    import time
    random.shuffle(tr_data_doi)
    results = []
    for tr in tr_data_doi[0:50]:
        if tr['pmid'] is None and tr['pmcid'] is None and \
           tr['doi'] is not None:
            pubmed_res = pubmed_client.get_ids(f'{tr["doi"]}[AID]')
            time.sleep(1)
            #id_res = id_lookup(tr['doi'], 'doi')
            #print(id_res)
            #tr.update(id_res)
            results.append(pubmed_res)
    """



