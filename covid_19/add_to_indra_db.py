import logging
from collections import defaultdict
from indra.util import batch_iter
from indra_db.util import get_db
from indra_db.util.data_gatherer import DataGatherer, DGContext
from indra_db.cli.content import ContentManager, logger as content_logger
from covid_19.get_indra_stmts import get_metadata_dict
from covid_19.preprocess import get_text_refs_from_metadata, \
    get_zip_texts_for_entry, download_latest_data, get_all_texts
from indra_db.databases import sql_expressions as sql_exp


logger = logging.getLogger(__name__)


content_logger.setLevel(logging.DEBUG)


gatherer = DataGatherer('content', ['refs', 'content'])


class Cord19Manager(ContentManager):
    tr_cols = ('pmid', 'pmcid', 'doi')
    my_source = 'cord19'
    primary_col = 'pmid'

    def __init__(self, cord_md):
        self.cord_md = cord_md
        self.tr_data = []
        self.tc_data = []
        self.tc_cols = ('text_ref_id', 'source', 'format', 'text_type',
                        'content',)
        self.review_fname = 'cord19_mgr_review.txt'

        texts_by_file = get_all_texts()
        # Get tr_data list from Cord19 metadata
        # tr_data is a list [{'pmid': xxx, 'pmcid': xxx}, {...}]
        # tc_data is a list of dictionaries keyed by column name (???)
        for ix, md_entry in enumerate(cord_md):
            if ix % 10000 == 0:
                print(f"Processing CORD-19 full texts: {ix} of {len(cord_md)}")
            text_refs = get_text_refs_from_metadata(md_entry)
            doi = text_refs.get('DOI')
            if doi is not None:
                doi = doi.upper()
            tr_data_entry = {'pmid': text_refs.get('PMID'),
                             'pmcid': text_refs.get('PMCID'),
                             'doi': doi,
                             'cord_uid': text_refs.get('CORD19_UID')}
            # If has abstract, add TC entry with abstract content
            tc_texts = get_zip_texts_for_entry(md_entry, texts_by_file)
            for source, text_type, text in tc_texts:
                tc_data_entry = {'source': source,
                                 'format': 'text',
                                 'text_type': text_type,
                                 'content': text}
                tc_data_entry.update(tr_data_entry)
                self.tc_data.append(tc_data_entry)
            self.tr_data.append(tr_data_entry)


    def filter_text_content(self, db, tc_data):
        """Link Text Content entries to corresponding Text Refs and filter
        out entries already in the database."""
        if not len(tc_data):
            return []
        logger.info("Beginning to filter text content...")
        tr_list = []
        # Step 1: Build a dictionary matching IDs to text ref objects
        for ix, tc_batch in enumerate(batch_iter(tc_data, 5000)):
            # Get the sets of IDs for this batch
            # Only use the generator once!
            ids = [(tc['pmid'], tc['pmcid'], tc['doi']) for tc in tc_batch]
            pmids, pmcids, dois = list(zip(*ids))
            # Remove any Nones and convert to sets
            pmid_set = set([i for i in pmids if i is not None])
            pmcid_set = set([i for i in pmcids if i is not None])
            doi_set = set([i for i in dois if i is not None])
            # Get all TextRefs for the CORD19 IDs
            logger.debug("Getting text refs for CORD19 articles")
            tr_list += db.select_all(db.TextRef, sql_exp.or_(
                            db.TextRef.pmid_in(pmid_set, filter_ids=True),
                            db.TextRef.pmcid_in(pmcid_set, filter_ids=True),
                            db.TextRef.doi_in(doi_set, filter_ids=True)))
        # Next, build dictionaries mapping IDs back to TextRef objects so
        # that we can link records in tc_data to TextRefs
        trs_by_doi = defaultdict(set)
        trs_by_pmc = defaultdict(set)
        trs_by_pmid = defaultdict(set)
        for tr in tr_list:
            if tr.doi:
                trs_by_doi[tr.doi].add(tr)
            if tr.pmcid:
                trs_by_pmc[tr.pmcid].add(tr)
            if tr.pmid:
                trs_by_pmid[tr.pmid].add(tr)

        # Now, build a new dictionary of text content including the TRIDs
        # rather than pmid/pmcid/doi
        # A list of dictionaries each containing: tr_id, source, format and
        # text_type
        flawed_tcs = set()
        tc_data_by_tr = []
        for tc_entry in tc_data:
            by_tr_entry = {}
            for field in ('source', 'format', 'text_type', 'content'):
                by_tr_entry[field] = tc_entry[field]
            tr_ids_for_tc = set()
            for id_type, trs_by_id in (('pmid', trs_by_pmid),
                                       ('pmcid', trs_by_pmc),
                                       ('doi', trs_by_doi)):
                tr_set = trs_by_id.get(tc_entry[id_type])
                if tr_set is not None:
                    # assert len(tr_set) == 1
                    if len(tr_set) != 1:
                        logger.warning(
                            '%s %s is associated with multiple TextRefs: %s'
                            % (id_type, tc_entry[id_type], tr_set))
                        continue
                    tr = list(tr_set)[0]
                    tr_ids_for_tc.add(tr.id)
            # Because this function is called using tc_data that has already
            # been filtered by text ref, we should always get unambiguous
            # matches to text_refs here.
            if len(tr_ids_for_tc) != 1:
                log_entry = (tc_entry['pmid'], tc_entry['pmcid'],
                             tc_entry['doi'], tc_entry['cord_uid'],
                             tuple(tr_ids_for_tc))
                logger.warning('Missing or ambiguous match to text ref: %s' %
                    str(log_entry))
                flawed_tcs.add(log_entry)
            else:
                tr_id = list(tr_ids_for_tc)[0]
                by_tr_entry['trid'] = tr_id
                tc_data_by_tr.append(by_tr_entry)

        # Step 2: Get existing Text Content objects corresponding to the
        # the given text refs with the same format and source.
        # This should be a very small list, in general.
        existing_tc_records = []
        for source, text_type in (('cord19_abstract', 'abstract'),
                                  ('cord19_pmc_xml', 'fulltext'),
                                  ('cord19_pdf', 'fulltext')):
            logger.debug('Finding existing text content from db for '
                         'source type %s' % source)
            tc_by_source = [tc_entry for tc_entry in tc_data_by_tr
                                     if tc_entry['source'] == source]
            existing_tcs = db.select_all(
                db.TextContent,
                db.TextContent.text_ref_id.in_([
                                tc['trid'] for tc in tc_by_source]),
                db.TextContent.source == source,
                db.TextContent.format == 'text',
                db.TextContent.text_type == text_type
            )
            # Reformat Text Content objects to list of tuples
            existing_tc_records += [
                (tc.text_ref_id, tc.source, tc.format, tc.text_type)
                for tc in existing_tcs
                ]
            logger.debug("Found %d existing records on the db for %s." %
                         (len(existing_tc_records), source))
        # Convert list of dicts into a list of tuples
        tc_records = []
        for tc_entry in tc_data_by_tr:
            tc_records.append((tc_entry['trid'], tc_entry['source'],
                               tc_entry['format'], tc_entry['text_type'],
                               tc_entry['content']))

        # Filter the TC records to exclude
        filtered_tc_records = [
           rec for rec in tc_records if rec[:-1] not in existing_tc_records
        ]
        logger.info("Finished filtering the text content.")
        return list(set(filtered_tc_records)), flawed_tcs

    @ContentManager._record_for_review
    @DGContext.wrap(gatherer)
    def populate(self, db):
        # Turn the list of dicts into a set of tuples
        tr_data_set = {tuple([entry[id_type] for id_type in self.tr_cols])
                       for entry in self.tr_data}
        # Filter_text_refs will figure out which articles are already in the
        # TextRef table and will update them with any new metadata;
        # filtered_tr_records are the ones that need to be added to the DB
        filtered_tr_records = set()
        flawed_tr_records = set()
        for ix, tr_batch in enumerate(batch_iter(tr_data_set, 10000)):
            print("Getting Text Refs using pmid/pmcid/doi, batch", ix)
            filt_batch, flaw_batch, id_map = \
                    self.filter_text_refs(db, set(tr_batch),
                                    primary_id_types=['pmid', 'pmcid', 'doi'])
            filtered_tr_records |= set(filt_batch)
            flawed_tr_records |= set(flaw_batch)
        trs_to_skip = {rec for cause, rec in flawed_tr_records}
        # Why did the original version not skip in case of disagreeing
        # pmid or doi?
        #pmcids_to_skip = {rec[self.tr_cols.index('pmcid')]
        #                  for cause, rec in flawed_tr_records
        #                  if cause in ['pmcid', 'over_match_input',
        #                               'over_match_db']}

        # Then we put together the updated text content data
        if len(trs_to_skip) != 0:
            mod_tc_data = [
                tc for tc in self.tc_data
                if (tc.get('pmid'), tc.get('pmcid'), tc.get('doi'))
                                                    not in trs_to_skip]
        else:
            mod_tc_data = self.tc_data

        # Upload TextRef data for articles NOT already in the DB
        logger.info('Adding %d new text refs...' % len(filtered_tr_records))
        if filtered_tr_records:
            self.upload_text_refs(db, filtered_tr_records)
        gatherer.add('refs', len(filtered_tr_records))

        # Process the text content data
        filtered_tc_records, flawed_tcs = \
                            self.filter_text_content(db, mod_tc_data)

        # Upload the text content data.
        logger.info('Adding %d more text content entries...' %
                    len(filtered_tc_records))
        self.upload_text_content(db, filtered_tc_records)
        gatherer.add('content', len(filtered_tc_records))
        return {'filtered_tr_records': filtered_tr_records,
                'flawed_tr_records': flawed_tr_records,
                'mod_tc_data': mod_tc_data,
                'filtered_tc_records': filtered_tc_records}


if __name__ == '__main__':
    download_latest_data()
    md = get_metadata_dict()
    md = [e for e in md if e['doi'] and
                           e['doi'].upper() != '0.1126/SCIENCE.ABB7331']
    cm = Cord19Manager(md)
    db = get_db('primary')
    res = cm.populate(db)

