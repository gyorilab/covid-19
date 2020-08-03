import tqdm
import pickle
import gilda
import logging
from collections import Counter
from indra.util import write_unicode_csv
from indra.statements import Complex
from indra.tools import assemble_corpus as ac
from indra.databases import get_identifiers_url
from indra.ontology.standardize import get_standard_name


logging.getLogger('gilda').setLevel(logging.WARNING)


def get_eidos_gilda_grounding_counts(stmts):
    """Return normalized text counts (name in case of Eidos concepts)
    and evidence texts corresponding to each agent text."""
    texts = []
    ev_text_for_agent_text = {}
    for stmt in stmts:
        for agent in stmt.agent_list():
            txt = agent.name
            matches = gilda.ground(txt)
            if matches:
                gr = matches[0].term.db, matches[0].term.id
            else:
                gr = None, None
            standard_name = get_standard_name(*gr) \
                if gr[0] is not None else ''
            url = get_identifiers_url(*gr) if gr[0] is not None else ''
            ev_text_for_agent_text[txt] = (stmt.evidence[0].pmid,
                                           stmt.evidence[0].text)
            texts.append((txt, ('%s:%s' % gr) if gr[0] else '',
                          standard_name, url, ''))
    # Count the unique text-grounding entries
    cnt = Counter(texts)
    return cnt, ev_text_for_agent_text


def get_text_grounding_counts(stmts):
    """Return countss of entity texts and evidence texts for those
    entity texts."""
    texts = []
    ev_text_for_agent_text = {}
    # Iterate over each statement and its agents
    stmts = ac.map_grounding(stmts)
    for stmt in tqdm.tqdm(stmts):
        for idx, agent in enumerate(stmt.agent_list()):
            if agent is None or 'TEXT' not in agent.db_refs:
                continue
            # Get some properties of the assembled agent (grounding,
            # standard name, link-out URL)
            gr = agent.get_grounding()
            url = get_identifiers_url(*gr) if gr[0] is not None else ''
            agent_txt = agent.db_refs['TEXT']
            ev_text_for_agent_text[agent_txt] = (stmt.evidence[0].pmid,
                                                 stmt.evidence[0].text)
            gilda_grounding = gilda.ground(agent_txt)
            gilda_grounding = '%s:%s' % (gilda_grounding[0].term.db,
                                         gilda_grounding[0].term.id) \
                if gilda_grounding else ''
            # We now add a new entry to the text-grounding list
            texts.append((agent_txt, ('%s:%s' % gr) if gr[0] else '',
                          agent.name, url, gilda_grounding))
    # Count the unique text-grounding entries
    cnt = Counter(texts)
    return cnt, ev_text_for_agent_text


def get_raw_statement_text_grounding_counts(stmts):
    texts = []
    ev_text_for_agent_text = {}
    for stmt in stmts:
        for agent in stmt.agent_list():
            if agent is None:
                continue
            if stmt.evidence[0].source_api == 'eidos':
                txt = agent.db_refs['TEXT_NORM']
            else:
                txt = agent.db_refs['TEXT']
            ev_text_for_agent_text[txt] = (stmt.evidence[0].pmid,
                                           stmt.evidence[0].text)
            assert txt, agent.db_refs
            gr = agent.get_grounding()
            standard_name = get_standard_name(*gr) if gr[0] else ''
            url = get_identifiers_url(*gr) if gr[0] is not None else ''
            gilda_grounding = gilda.ground(txt, context=stmt.evidence[0].text)
            gilda_grounding = '%s:%s' % (gilda_grounding[0].term.db,
                                         gilda_grounding[0].term.id) \
                if gilda_grounding else ''
            texts.append((txt, ('%s:%s' % gr) if gr[0] else '',
                          standard_name, url, gilda_grounding))
    cnt = Counter(texts)
    return cnt, ev_text_for_agent_text


def load_stmts(fname):
    # Load statements
    with open(fname, 'rb') as fh:
        stmts = pickle.load(fh)
    return stmts


def dump_table(text_grounding_cnt, ev_text_for_agent_text, fname):
    # Dump the results into a TSV file
    rows = [['text', 'grounding', 'standard_name', 'url', 'gilda_grounding',
             'count', 'pmid', 'ev_text']]
    for data, count in text_grounding_cnt.most_common():
        pmid, ev_text = ev_text_for_agent_text[data[0]]
        row = list(data) + [str(count), pmid, ev_text]
        rows.append(row)
    write_unicode_csv(fname, rows, delimiter='\t')


if __name__ == '__main__':
    #stmts = load_stmts('../cord19_pmc_stmts_filt.pkl')
    #cnt, ev_text_for_agent_text = get_text_grounding_counts(stmts)
    #dump_table(cnt, ev_text_for_agent_text, '../grounding_table.tsv')

    #stmts = load_stmts('../eidos_statements.pkl')
    #cnt, ev_text_for_agent_text = get_eidos_gilda_grounding_counts(stmts)
    #dump_table(cnt, ev_text_for_agent_text,
    #           '../grounding_eidos_gilda_table.tsv')

    stmts = load_stmts('../reach_cord19_new.pkl')
    cnt, ev_text_for_agent_text = get_text_grounding_counts(stmts)
    dump_table(cnt, ev_text_for_agent_text, '../reach_new_grounding_table.tsv')
