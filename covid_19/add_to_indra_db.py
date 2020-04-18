import logging
import pandas as pd
from indra_db import get_primary_db
from indra.literature import id_lookup
from indra.literature import pubmed_client
from indra_db.managers import content_manager
from indra_db.managers.content_manager import PmcManager
from covid_19.get_indra_stmts import get_unique_text_refs, get_metadata_dict, \
                                     cord19_metadata_for_trs
from covid_19.preprocess import get_text_refs_from_metadata

logger = logging.getLogger(__name__)


content_manager.logger.setLevel(logging.DEBUG)


"""
def get_val(row, col):
    df_cols = [
      'index', 'sha', 'source_x', 'title', 'doi', 'pmcid', 'pubmed_id',
      'license', 'abstract', 'publish_time', 'authors', 'journal',
      'Microsoft Academic Paper ID', 'WHO #Covidence', 'has_full_text',
      'full_text_file', 'content_path', 'content_type']
    val = row[df_cols.index(col)]
    return None if pd.isna(val) else val
"""

class Cord19Manager(PmcManager):
    my_source = 'cord19'
    #tr_cols = ('pmid', 'pmcid', 'doi', 'cord19_uid')
    tr_cols = ('pmid', 'pmcid', 'doi')

    def __init__(self, cord_md):
        self.cord_md = cord_md
        self.tr_data = []
        self.review_fname = 'foo.txt'

        # Get tr_data list from Cord19 metadata
        # tr_data is a list [{'pmid': xxx, 'pmcid': xxx}, {...}]
        for md_entry in cord_md:
            text_refs = get_text_refs_from_metadata(md_entry)
            doi = text_refs.get('DOI')
            if doi is not None:
                doi = doi.upper()
            tr_data_entry = {'pmid': text_refs.get('PMID'),
                             'pmcid': text_refs.get('PMCID'),
                             'doi': doi}
            self.tr_data.append(tr_data_entry)

        """
        self.cord_trs = get_unique_text_refs()
        # Dict keyed by text ref ID 
        self.tr_dicts = cord19_metadata_for_trs(self.cord_trs, cord_md)
        ref_key_map = {'PMID': 'pmid'}
        for tr_id, tr_refs in self.tr_dicts.items():
            tr_data_dict = {'pmid': None, 'pmcid': None, 'doi': None,
                            'manuscript_id': None}
            for key, val in tr_refs.items():
                if key not in ref_key_map:
                    continue
                mapped_key = ref_key_map[key]
                tr_data_dict[mapped_key] = val
            self.tr_data.append(tr_data_dict)
        """

    def populate(self, db):

        # Turn the list of dicts into a set of tuples
        tr_data_set = {tuple([entry[id_type] for id_type in self.tr_cols])
                       for entry in self.tr_data}
        # FIXME HACK: Manually remove the broken DOI
        tr_data_set = set([t for t in tr_data_set
                             if t[2] != '0.1126/SCIENCE.ABB7331'])
        # Filter_text_refs will figure out which articles are already in the
        # TextRef table and will update them with any new metadata
        filtered_tr_records = []
        flawed_tr_records = []
        for ix, tr_batch in enumerate(batch_iter(tr_data_set)):
            print("Getting Text Refs using pmid/pmcid/doi, batch", ix)
            filt_batch, flaw_batch = \
                    self.filter_text_refs(db,a tr_batch,
                                    primary_id_types=['pmid', 'pmcid', 'doi'])
            filtered_tr_records.extend(filt_batch)
            flawed_tr_records.extend(flaw_batch)

        import ipdb; ipdb.set_trace()
        pmcids_to_skip = {rec[self.tr_cols.index('pmcid')]
                          for cause, rec in flawed_tr_records
                          if cause in ['pmcid', 'over_match_input',
                                       'over_match_db']}
        if len(pmcids_to_skip) is not 0:
            mod_tc_data = [
                tc for tc in tc_data if tc['pmcid'] not in pmcids_to_skip
                ]
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
        gatherer.add('refs', len(filtered_tr_records))

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


    """
    def upload_batch(self):
        # Turn the list of dicts into a set of tuples
        tr_data_set = {tuple([entry[id_type] for id_type in self.tr_cols])
                       for entry in tr_data}

        filtered_tr_records, flawed_tr_records = \
            self.filter_text_refs(db, tr_data_set,
                                  primary_id_types=['pmid', 'pmcid',
                                                    'manuscript_id'])

       a 


    def upload_batch(self, db, tr_data, tc_data, get_missing_pmids=True):
        # Modifies the tr_data dictionary in place
        if get_missing_pmids:
            tr_data = self.get_missing_pmids(db, tr_data)


    def get_missing_pmids(self, db, tr_data):
        tr_data_pmid = []
        tr_data_pmcid = []
        tr_data_doi = []
        tr_data_none = []
        for tr in tr_data:
            if tr['pmid'] is not None:
                tr_data_pmid.append(tr)
            else:
                if tr['pmcid'] is not None:
                    tr_data_pmcid.append(tr)
                else:
                    if tr['doi'] is not None:
                        tr_data_doi.append(tr)
                    else:
                        tr_data_none.append(tr)
        print(len(tr_data_pmcid), 'pmcid but no pmid')
        # Fill in PMIDs for the papers for which we have a PMCID
        super(Cord19Manager, self).get_missing_pmids(db, tr_data_pmcid)
        # Fill in PMIDs for the papers for which we have a DOI (or nothing)
        tr_list = db.select_all(
            db.TextRef, db.TextRef.doi.in_(
                [tr_entry['doi'] for tr_entry in tr_data_doi]
                )
            )
        pmids_from_db = {tr.doi: tr.pmid for tr in tr_list
                         if tr.pmid is not None}

        logger.debug("Found %d pmids on the databse." % len(pmids_from_db))
        num_found_non_db = 0
        for tr_entry in tr_data_doi:
            if tr_entry['doi'] not in pmids_from_db.keys():
                pass
                #ret = id_lookup(tr_entry['pmcid'], idtype='pmcid')
                #if 'pmid' in ret.keys() and ret['pmid'] is not None:
                #    tr_entry['pmid'] = ret['pmid']
                #    num_found_non_db += 1
                #    num_missing -= 1
            else:
                tr_entry['pmid'] = pmids_from_db[tr_entry['doi']]
                #num_missing -= 1
        tr_combined = tr_data_pmid + tr_data_pmcid + tr_data_doi + tr_data_none
        tr_no_pmid = []
        for tr in tr_combined:
            if tr['pmid'] is None:
                tr_no_pmid.append(tr)
        print(len(tr_no_pmid), 'no pmid')
        return tr_combined
    """


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



