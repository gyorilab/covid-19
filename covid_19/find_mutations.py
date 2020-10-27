import re
import csv
from covid_19.preprocess import get_metadata_dict, get_zip_texts_for_entry, \
                                get_metadata_df, get_all_texts
from indra_db.util import get_db


covid_docs_file = '../covid_docs_ranked_corona.csv'
covid_pmids = set()
with open(covid_docs_file, 'rt') as f:
    csv_reader = csv.reader(f, delimiter=',')
    for row in csv_reader:
        pmid = row[4]
        covid_pmids.add(pmid)


md = get_metadata_dict()


aa_reg = '[ACDEFGHIKLMNPQRSTVWY]'
mut_reg = '\s+' + aa_reg + '\d+' + aa_reg + '\s+'
print(mut_reg)

aa_short = ['ala', 'arg', 'asn', 'asp', 'cys', 'gln', 'glu', 'gly',
            'his', 'ile', 'leu', 'lys', 'met', 'phe', 'pro', 'ser',
            'thr', 'trp', 'tyr', 'val']
aa_short_reg = '|'.join([aa for aa in aa_short])
aa_seq_reg = '(?:%s)\d{2,5}' % aa_short_reg
print(aa_seq_reg)

ignore_list = (
    'Y2H', # Yeast two-hybrid
    'C3H', # Mouse strain
    'D980R', # HeLa cell strain
    'E3L', # vaccinia virus E3L
    'S1P', # Sphingosine-1-phosphate
    'Q7R', # quercetin 7-rhamnoside
    'S6K', # S6 kinase
)


texts_by_file = get_all_texts()
by_mut = {}
by_doc = {}
for ix, md_entry in enumerate(md):
    pmid = md_entry['pubmed_id']
    title = md_entry['title']
    if pmid not in covid_pmids:
        continue
    texts = get_zip_texts_for_entry(md_entry, texts_by_file, zip=False)
    cord_uid = md_entry['cord_uid']
    for _, text_type, text in texts:
        matches = re.findall(mut_reg, text, flags=re.IGNORECASE)
        for match in matches:
            ms = match.strip()
            if re.match('H\dN', ms) or re.match('S\d[ABCDEG]', ms) or \
               ms in ignore_list:
                continue
            if ms not in by_mut:
                by_mut[ms] = set([(title, pmid)])
            else:
                by_mut[ms].add((title, pmid))
            if (title, pmid) not in by_doc:
                by_doc[(title, pmid)] = set([ms])
            else:
                by_doc[(title, pmid)].add(ms)


docs = sorted([(k, list(v)) for k, v in by_doc.items()],
               key=lambda x: len(x[1]), reverse=True)
muts = sorted([(k, list(v)) for k, v in by_mut.items()],
               key=lambda x: len(x[1]), reverse=True)


def dump_docs(docs_sorted):
    docs_rows = [['title', 'pmid', 'pmid_link', 'mutation_count', 'mutation']]
    for (title, pmid), muts in docs_sorted:
        pmid_link = f'https://www.ncbi.nlm.nih.gov/pubmed/{pmid}'
        count = len(muts)
        for mut in muts:
            docs_rows.append([title, pmid, pmid_link, count, mut])
    with open('docs_ranked_by_muts.csv', 'wt') as f:
        csvwriter = csv.writer(f, delimiter=',')
        csvwriter.writerows(docs_rows)


def dump_muts(muts_sorted):
    muts_rows = [['mutation', 'doc_count', 'title', 'pmid', 'pmid_link']]
    for mut, docs in muts_sorted:
        count = len(docs)
        for title, pmid in docs:
            pmid_link = f'https://www.ncbi.nlm.nih.gov/pubmed/{pmid}'
            muts_rows.append([mut, count, title, pmid, pmid_link])
    with open('muts_ranked_by_docs.csv', 'wt') as f:
        csvwriter = csv.writer(f, delimiter=',')
        csvwriter.writerows(muts_rows)

dump_docs(docs)
dump_muts(muts)



