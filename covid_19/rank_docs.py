import csv
import argparse
from tabulate import tabulate
from indra.literature import crossref_client, pubmed_client
from indra.preassembler import Preassembler
from indra.preassembler.hierarchy_manager import hierarchies
from indra.tools import assemble_corpus as ac
from indra.databases.mesh_client import mesh_id_to_tree_numbers, get_mesh_name
from indra_db import get_primary_db
from covid_19.emmaa_update import stmts_by_text_refs
from covid_19.preprocess import get_metadata_dict

_cord_by_doi = {}
_cord_by_pmid = {}
_mesh_tree_to_id = {}


def get_mesh_tree_to_id():
    global _mesh_tree_to_id
    if not _mesh_tree_to_id:
        for mesh_id, tree_num_list in mesh_id_to_tree_numbers.items():
            for tree_num in tree_num_list:
                _mesh_tree_to_id[tree_num] = mesh_id
    return _mesh_tree_to_id


def get_mesh_children(mesh_id):
    parent_tns = mesh_id_to_tree_numbers[mesh_id]
    children = []
    mesh_tree_to_id = get_mesh_tree_to_id()
    for parent_tn in parent_tns:
        for tn, m_id in mesh_tree_to_id.items():
            if tn.startswith(parent_tn):
                children.append((m_id, get_mesh_name(m_id)))
    return children


def get_cord_info():
    global _cord_by_doi
    global _cord_by_pmid
    if not (_cord_by_doi and _cord_by_pmid):
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
    if ev_tr_dict.get('DOI'):
        doi = ev_tr_dict['DOI']
        cord_entry = cord_by_doi.get(doi)
        if cord_entry:
            return (cord_entry['title'], cord_entry['authors'],
                    cord_entry['journal'], cord_entry['publish_time'].year)
        # Article not in CORD-19 corpus, get metadata from Crossref
        print("Querying crossref")
        cr_entry = crossref_client.get_metadata(doi)
        if cr_entry:
            try:
                author_str = '; '.join([
                    f"{auth['family']}, {auth.get('given', '')}"
                    for auth in cr_entry['author']])
            except KeyError:
                try:
                    author_str = '; '.join([
                        f"{auth['name']}" for auth in cr_entry['author']])
                except KeyError:
                    author_str = ''
            title_list = cr_entry['title']
            if title_list:
                title = title_list[0]
            container_list = cr_entry['container-title']
            if container_list:
                journal = container_list[0]
            return (title, author_str, journal,
                    cr_entry['issued']['date-parts'][0][0])
    # If we got here, then we haven't found the metadata yet, try by PMID
    if ev_tr_dict.get('PMID'):
        pmid = ev_tr_dict['PMID']
        cord_entry = cord_by_pmid.get(pmid)
        if cord_entry:
            return (cord_entry['title'], cord_entry['authors'],
                    cord_entry['journal'], cord_entry['publish_time'].year)
        print("Querying Pubmed")
        pm_entry = pubmed_client.get_metadata_for_ids([pmid])
        if pm_entry:
            pm_md = pm_entry[pmid]
            author_str = '; '.join(pm_md['authors'])
            return (pm_md['title'], author_str,
                    pm_md.get('journal_title', ''),
                    pm_md['publication_date']['year'])
    # No luck, return empty strings
    return ('', '', '', '')


def num_molecular_stmts(stmt_list):
    def has_molecular_grounding(ag):
        if ag is None:
            return False
        return True if set(['FPLX', 'CHEBI', 'PUBCHEM', 'UP']).intersection(
                                    set(ag.db_refs.keys())) else False
    return sum([1 if all([has_molecular_grounding(ag)
                         for ag in stmt.agent_list()]) else 0
                  for stmt in stmt_list])


def dump_doc_files(tr_stmts, output_file_base):
    # Index cord content 
    tr_rows = [('title', 'authors', 'journal', 'year', 'pmid', 'pmcid', 'doi',
                'indra_db_id', 'url', 'num_uniq_stmts', 'num_molecular_stmts',
                'total_stmts')]
    for ix, (tr, stmt_list) in enumerate(tr_stmts):
        if ix % 100 == 0:
            print('-------', ix, '--------')
        text_refs = stmt_list[0].evidence[0].text_refs
        title, authors, journal, date = get_tr_metadata(text_refs)
        doi = text_refs.get('DOI')
        trid = text_refs.get('TRID')
        url = f'https://dx.doi.org/{doi}' if doi else ''
        num_ev = sum([len(s.evidence) for s in stmt_list])
        num_mol = num_molecular_stmts(stmt_list)
        tr_rows.append((title, authors, journal, date,
                        text_refs.get('PMID', ''),
                        text_refs.get('PMCID', ''),
                        doi, trid, url, len(stmt_list), num_mol, num_ev))
    # CSV
    with open(f'{output_file_base}.csv', 'wt') as f:
        csvwriter = csv.writer(f, delimiter=',')
        csvwriter.writerows(tr_rows)
    # HTML
    html_table = tabulate(tr_rows, tablefmt='html')
    with open(f'{output_file_base}.html', 'wt') as f:
        f.write(html_table)


    

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

    # Filter to MESH term

    all_refs_file_base = f'{args.output_base}_all'
    dump_doc_files(trs_sorted, all_refs_file_base)



