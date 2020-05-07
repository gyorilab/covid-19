import csv
import argparse
from indra.preassembler import Preassembler
from indra.preassembler.hierarchy_manager import hierarchies
from indra.tools import assemble_corpus as ac
from covid_19.emmaa_update import stmts_by_text_refs
from covid_19.preprocess import get_metadata_dict


#def filter_trs_to_mesh(by_tr, 
_cord_by_doi = {}
_cord_by_pmid = {}

def get_cord_info():
    global _cord_by_doi
    global _cord_by_pmid
    if _cord_by_doi and _cord_by_pmid:
        return (_cord_by_doi, _cord_by_pmid)
    cord_md = get_metadata_dict()
    for md_entry in cord_md:
        if md_entry.get('doi'):
            _cord_by_doi[md_entry['doi'].upper()] = md_entry
        if md_entry.get('pubmed_id'):
            _cord_by_pmid[md_entry['pubmed_id']] = md_entry
    return (_cord_by_doi, _cord_by_pmid)



def get_tr_metadata(ev_tr_dict):
    cord_by_doi, cord_by_pmid = get_cord_info()
    # If has DOI, look up in CORD19
    title, authors, journal, date = (None, None, None, None)
    if 'DOI' in ev_tr_dict:
        cord_entry = cord_by_doi.get(ev_tr_dict['DOI'])
        if cord_entry:
            title = cord_entry['title']
            authors = cord_entry['authors']
            journal = cord_entry['journal']
            date = cord_entry['publish_time']
        # else fallbackto crossref query
    elif 'PMID' in ev_tr_dict:
        cord_entry = cord_by_pmid.get(ev_tr_dict['PMID'])
        if cord_entry:
            title = cord_entry['title']
            authors = cord_entry['authors']
            journal = cord_entry['journal']
            date = cord_entry['publish_time']
        # else fallback to pubmed query
    return title, authors, journal, date


def dump_refs_to_csv(tr_stmts, output_file):
    # Index cord content 
    tr_rows = [('title', 'authors', 'journal', 'date', 'pmid', 'pmcid', 'doi',
                'url', 'num_uniq_stmts', 'total_stmts')]
    for tr, stmt_list in tr_stmts:
        text_refs = stmt_list[0].evidence[0].text_refs
        title, authors, journal, date = get_tr_metadata(text_refs)
        doi = text_refs.get('DOI')
        url = f'https://dx.doi.org/{doi}' if doi else ''
        num_ev = sum([len(s.evidence) for s in stmt_list])
        tr_rows.append((title, authors, journal, date, text_refs.get('PMID', ''),
                        text_refs.get('PMCID', ''),
                        doi, url, num_ev, len(stmt_list)))
    with open(output_file, 'wt') as f:
        csvwriter = csv.writer(f, delimiter=',')
        csvwriter.writerows(tr_rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Generate ranked lists of COVID docs for curation.')
    parser.add_argument('-i', '--input_file', help='Name of stmt pkl file',
                        required=True)
    parser.add_argument('-o', '--output_base',
                        help='Basename for output files.', required=True)
    args = parser.parse_args()

    # Load statements and filter to grounded only
    stmts = ac.load_statements(args.input_file)
    stmts = ac.filter_grounded_only(stmts)

    # Sort by TextRefs
    by_tr, no_tr = stmts_by_text_refs(stmts)

    # Combine duplicates in each statement list
    by_tr_pa = {}
    for tr, stmt_list in by_tr.items():
        pa = Preassembler(hierarchies, stmt_list)
        uniq_stmts = pa.combine_duplicates()
        by_tr_pa[tr] = uniq_stmts

    # Sort text refs by numbers of statements
    trs_sorted = sorted([(tr, stmt_list) for tr, stmt_list in by_tr_pa.items()],
                       key=lambda x: len(x[1]), reverse=True)

    all_refs_file = f'{args.output_base}_all.csv'
    dump_refs_to_csv(trs_sorted, all_refs_file)


