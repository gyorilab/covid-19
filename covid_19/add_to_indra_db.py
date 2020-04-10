import logging
import pandas as pd
from indra_db import get_primary_db
from indra.literature import id_lookup
from indra.literature import pubmed_client
from indra_db.managers import content_manager
from indra_db.managers.content_manager import PmcManager
from covid_19.get_text import get_file_data


logger = logging.getLogger(__name__)


content_manager.logger.setLevel(logging.DEBUG)


def get_val(row, col):
    """Get the value of the pandas row given the column name."""
    df_cols = [
      'index', 'sha', 'source_x', 'title', 'doi', 'pmcid', 'pubmed_id',
      'license', 'abstract', 'publish_time', 'authors', 'journal',
      'Microsoft Academic Paper ID', 'WHO #Covidence', 'has_full_text',
      'full_text_file', 'content_path', 'content_type']
    val = row[df_cols.index(col)]
    return None if pd.isna(val) else val


class Cord19Manager(PmcManager):

    def upload_batch(self, db, tr_data, tc_data, get_missing_pmids=True):
        # Modifies the tr_data dictionary in place
        if get_missing_pmids:
            tr_data = self.get_missing_pmids(db, tr_data)


    def get_missing_pmids(self, db, tr_data):
        import ipdb; ipdb.set_trace()
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
                """
                ret = id_lookup(tr_entry['pmcid'], idtype='pmcid')
                if 'pmid' in ret.keys() and ret['pmid'] is not None:
                    tr_entry['pmid'] = ret['pmid']
                    num_found_non_db += 1
                    num_missing -= 1
                """
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



if __name__ == '__main__':
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




